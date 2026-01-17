// SPDX-License-Identifier: BSD-3-Clause

#include <arpa/inet.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include "protocol/components.h"

#define DISPATCHER_DIR ".dispatcher"
#define PIPES_DIR ".pipes"
#define INSTALL_REQ_PIPE DISPATCHER_DIR "/install_req_pipe"
#define CONNECTION_REQ_PIPE DISPATCHER_DIR "/connection_req_pipe"
#define PIPE_PERMISSIONS 0600
#define MAX_PIPES 600

/* Service registry entry */
typedef struct {
	char access_path[ACCESS_PATH_LENGTH];
	char call_pipe[COMM_PIPE_NAME_LENGTH];
	char return_pipe[COMM_PIPE_NAME_LENGTH];
	char version[VERSION_LENGTH];
	uint8_t version_len;
	uint16_t call_pipe_len;
	uint16_t return_pipe_len;
} ServiceEntry;

/* Global service registry with mutex */
static ServiceEntry *g_services = NULL;
static int g_service_count = 0;
static pthread_mutex_t g_services_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Global pipe counter with mutex */
static size_t g_pipe_count = 0;
static pthread_mutex_t g_pipes_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Worker thread argument for install */
typedef struct {
	char install_pipe_name[INSTALL_PIPE_NAME_LENGTH];
} InstallWorkerArg;

/* Worker thread argument for connection */
typedef struct {
	char response_pipe_name[CONNECT_PIPE_NAME_LENGTH];
	char access_path[ACCESS_PATH_LENGTH];
} ConnectWorkerArg;

/* Thread-safe service registry functions */
static int find_service_by_path(const char *access_path, ServiceEntry *out)
{
	int found = -1;

	pthread_mutex_lock(&g_services_mutex);
	for (int i = 0; i < g_service_count; i++) {
		if (strcmp(g_services[i].access_path, access_path) == 0) {
			if (out)
				memcpy(out, &g_services[i], sizeof(ServiceEntry));
			found = i;
			break;
		}
	}
	pthread_mutex_unlock(&g_services_mutex);
	return found;
}

static int register_service(const char *access_path, const char *call_pipe,
			    const char *return_pipe, const char *version,
			    uint8_t version_len, uint16_t cpn_len, uint16_t rpn_len)
{
	pthread_mutex_lock(&g_services_mutex);

	ServiceEntry *new_services =
		realloc(g_services, (g_service_count + 1) * sizeof(ServiceEntry));
	if (!new_services) {
		pthread_mutex_unlock(&g_services_mutex);
		return -1;
	}

	g_services = new_services;
	ServiceEntry *entry = &g_services[g_service_count];

	memset(entry, 0, sizeof(ServiceEntry));
	memcpy(entry->access_path, access_path, strlen(access_path));
	memcpy(entry->call_pipe, call_pipe, cpn_len);
	memcpy(entry->return_pipe, return_pipe, rpn_len);
	memcpy(entry->version, version, version_len);
	entry->version_len = version_len;
	entry->call_pipe_len = cpn_len;
	entry->return_pipe_len = rpn_len;

	g_service_count++;
	pthread_mutex_unlock(&g_services_mutex);
	return 0;
}

/* Utility functions */
static int ensure_directory(const char *dir)
{
	if (mkdir(dir, 0755) == -1 && errno != EEXIST) {
		perror("mkdir");
		return -1;
	}
	return 0;
}

static int create_pipe(const char *name, bool *created)
{
	struct stat st;

	if (created)
		*created = false;

	if (lstat(name, &st) == 0 && S_ISFIFO(st.st_mode))
		return 0;

	pthread_mutex_lock(&g_pipes_mutex);
	if (g_pipe_count >= MAX_PIPES) {
		pthread_mutex_unlock(&g_pipes_mutex);
		errno = EMFILE;
		return -1;
	}
	pthread_mutex_unlock(&g_pipes_mutex);

	if (mkfifo(name, PIPE_PERMISSIONS) == -1) {
		if (errno != EEXIST) {
			perror("mkfifo");
			return -1;
		}
		return 0;
	}

	pthread_mutex_lock(&g_pipes_mutex);
	g_pipe_count++;
	pthread_mutex_unlock(&g_pipes_mutex);

	if (created)
		*created = true;

	return 0;
}

static void forget_pipe(void)
{
	pthread_mutex_lock(&g_pipes_mutex);
	if (g_pipe_count > 0)
		g_pipe_count--;
	pthread_mutex_unlock(&g_pipes_mutex);
}

static void cleanup_pipe_dir(const char *dir_path)
{
	DIR *dir = opendir(dir_path);

	if (!dir)
		return;

	struct dirent *entry;
	while ((entry = readdir(dir)) != NULL) {
		if (strcmp(entry->d_name, ".") == 0 ||
		    strcmp(entry->d_name, "..") == 0)
			continue;

		char path[512];
		struct stat st;

		snprintf(path, sizeof(path), "%s/%s", dir_path, entry->d_name);
		if (lstat(path, &st) == -1)
			continue;

		if (S_ISFIFO(st.st_mode))
			unlink(path);
	}
	closedir(dir);
}

static void cleanup_pipes(void)
{
	cleanup_pipe_dir(PIPES_DIR);
	cleanup_pipe_dir(DISPATCHER_DIR);

	pthread_mutex_lock(&g_pipes_mutex);
	g_pipe_count = 0;
	pthread_mutex_unlock(&g_pipes_mutex);
}

/* Helper to read exactly n bytes */
static ssize_t read_all(int fd, void *buf, size_t count)
{
	size_t total = 0;
	char *ptr = buf;

	while (total < count) {
		ssize_t n = read(fd, ptr + total, count - total);

		if (n <= 0) {
			if (n == 0)
				return total; /* EOF */
			if (errno == EINTR)
				continue;
			return -1;
		}
		total += n;
	}
	return total;
}

/* Worker thread for handling a single install request */
static void *install_worker(void *arg)
{
	InstallWorkerArg *work = (InstallWorkerArg *)arg;

	pthread_detach(pthread_self());

	/* Create the install pipe if it doesn't exist */
	if (create_pipe(work->install_pipe_name, NULL) == -1) {
		free(work);
		return NULL;
	}

	/* Open and read the install header - blocks until server writes */
	int install_fd = open(work->install_pipe_name, O_RDONLY);

	if (install_fd == -1) {
		perror("open install_pipe");
		free(work);
		return NULL;
	}

	struct InstallHeader install_header;
	ssize_t n = read_all(install_fd, &install_header,
			     sizeof(struct InstallHeader));

	if (n != sizeof(struct InstallHeader)) {
		close(install_fd);
		free(work);
		return NULL;
	}

	/* Parse the install header - convert from big-endian */
	uint8_t version_len = install_header.m_VersionLen;
	uint16_t cpn_len = ntohs(install_header.m_CpnLen);
	uint16_t rpn_len = ntohs(install_header.m_RpnLen);
	uint16_t ap_len = ntohs(install_header.m_ApLen);

	/* Allocate buffer for contents */
	size_t total_len = version_len + cpn_len + rpn_len + ap_len;
	char *contents = malloc(total_len + 1);

	if (!contents) {
		close(install_fd);
		free(work);
		return NULL;
	}

	n = read_all(install_fd, contents, total_len);
	if (n != (ssize_t)total_len) {
		free(contents);
		close(install_fd);
		free(work);
		return NULL;
	}
	contents[total_len] = '\0';

	close(install_fd);

	/* Extract fields */
	char *version = contents;
	char *call_pipe = contents + version_len;
	char *return_pipe = call_pipe + cpn_len;
	char *access_path = return_pipe + rpn_len;

	/* Create null-terminated strings for registry */
	char access_path_str[ACCESS_PATH_LENGTH];
	char call_pipe_str[COMM_PIPE_NAME_LENGTH];
	char return_pipe_str[COMM_PIPE_NAME_LENGTH];

	memset(access_path_str, 0, sizeof(access_path_str));
	memcpy(access_path_str, access_path, ap_len);

	memset(call_pipe_str, 0, sizeof(call_pipe_str));
	memcpy(call_pipe_str, call_pipe, cpn_len);

	memset(return_pipe_str, 0, sizeof(return_pipe_str));
	memcpy(return_pipe_str, return_pipe, rpn_len);

	/* Create call and return pipes for the service */
	if (create_pipe(call_pipe_str, NULL) == -1) {
		free(contents);
		free(work);
		return NULL;
	}

	if (create_pipe(return_pipe_str, NULL) == -1) {
		free(contents);
		free(work);
		return NULL;
	}

	/* Open pipes in read-write mode to keep them alive and prevent SIGPIPE */
	int call_fd = open(call_pipe_str, O_RDWR | O_NONBLOCK);
	int return_fd = open(return_pipe_str, O_RDWR | O_NONBLOCK);

	/* We intentionally keep these file descriptors open
	 * to prevent SIGPIPE when clients disconnect */
	(void)call_fd;
	(void)return_fd;

	/* Register the service */
	register_service(access_path_str, call_pipe, return_pipe, version,
			 version_len, cpn_len, rpn_len);

	free(contents);
	free(work);
	return NULL;
}

/* Worker thread for handling a single connection request */
static void *connect_worker(void *arg)
{
	ConnectWorkerArg *work = (ConnectWorkerArg *)arg;
	bool created = false;

	pthread_detach(pthread_self());

	/* Find matching service */
	ServiceEntry service;
	int found = find_service_by_path(work->access_path, &service);

	/* Create response pipe */
	if (create_pipe(work->response_pipe_name, &created) == -1) {
		free(work);
		return NULL;
	}

	/* Open response pipe for writing - blocks until client opens for reading */
	int response_fd = open(work->response_pipe_name, O_WRONLY);

	if (response_fd == -1) {
		perror("open response pipe for writing");
		if (created)
			forget_pipe();
		unlink(work->response_pipe_name);
		free(work);
		return NULL;
	}

	if (found >= 0) {
		/* Prepare connection header */
		struct ConnectHeader conn_header;

		conn_header.m_VersionLen = service.version_len;
		conn_header.m_CpnLen = htonl(service.call_pipe_len);
		conn_header.m_RpnLen = htonl(service.return_pipe_len);

		/* Use iovec for efficient write */
		struct iovec iov[4];

		iov[0].iov_base = &conn_header;
		iov[0].iov_len = sizeof(struct ConnectHeader);
		iov[1].iov_base = service.version;
		iov[1].iov_len = service.version_len;
		iov[2].iov_base = service.call_pipe;
		iov[2].iov_len = service.call_pipe_len;
		iov[3].iov_base = service.return_pipe;
		iov[3].iov_len = service.return_pipe_len;

		if (writev(response_fd, iov, 4) < 0)
			perror("writev");
	}

	close(response_fd);

	/* Clean up the response pipe after client has read */
	unlink(work->response_pipe_name);
	if (created)
		forget_pipe();

	free(work);
	return NULL;
}

/* Main thread for listening to install requests */
static void *install_listener(void *arg)
{
	(void)arg;
	pthread_detach(pthread_self());

	while (1) {
		/* Open install_req_pipe for reading - blocks until a writer connects */
		int install_req_fd = open(INSTALL_REQ_PIPE, O_RDONLY);

		if (install_req_fd == -1) {
			perror("open install_req_pipe");
			sleep(1);
			continue;
		}

		while (1) {
			struct InstallRequestHeader req_header;
			ssize_t n = read_all(install_req_fd, &req_header,
					     sizeof(struct InstallRequestHeader));

			if (n <= 0) {
				/* EOF or error - reopen pipe */
				break;
			}

			if (n != sizeof(struct InstallRequestHeader))
				break;

			uint16_t ipn_len = ntohs(req_header.m_IpnLen);

			/* Read install pipe name */
			char install_pipe_name[INSTALL_PIPE_NAME_LENGTH];

			if (ipn_len >= INSTALL_PIPE_NAME_LENGTH)
				ipn_len = INSTALL_PIPE_NAME_LENGTH - 1;

			n = read_all(install_req_fd, install_pipe_name, ipn_len);
			if (n != (ssize_t)ipn_len)
				break;

			install_pipe_name[ipn_len] = '\0';

			/* Create worker thread to handle this install */
			InstallWorkerArg *work = malloc(sizeof(InstallWorkerArg));

			if (!work)
				continue;

			strncpy(work->install_pipe_name, install_pipe_name,
				INSTALL_PIPE_NAME_LENGTH - 1);
			work->install_pipe_name[INSTALL_PIPE_NAME_LENGTH - 1] = '\0';

			pthread_t worker;

			if (pthread_create(&worker, NULL, install_worker, work) != 0) {
				free(work);
				continue;
			}
		}

		close(install_req_fd);
	}

	return NULL;
}

/* Main thread for listening to connection requests */
static void *connect_listener(void *arg)
{
	(void)arg;
	pthread_detach(pthread_self());

	while (1) {
		/* Open connection_req_pipe for reading - blocks until a writer connects */
		int conn_req_fd = open(CONNECTION_REQ_PIPE, O_RDONLY);

		if (conn_req_fd == -1) {
			perror("open connection_req_pipe");
			sleep(1);
			continue;
		}

		while (1) {
			struct ConnectionRequestHeader req_header;
			ssize_t n = read_all(conn_req_fd, &req_header,
					     sizeof(struct ConnectionRequestHeader));

			if (n <= 0)
				break;

			if (n != sizeof(struct ConnectionRequestHeader))
				break;

			uint32_t rpn_len = ntohl(req_header.m_RpnLen);
			uint32_t ap_len = ntohl(req_header.m_ApLen);

			/* Read response pipe name and access path */
			char response_pipe_name[CONNECT_PIPE_NAME_LENGTH];
			char access_path[ACCESS_PATH_LENGTH];

			if (rpn_len >= CONNECT_PIPE_NAME_LENGTH)
				rpn_len = CONNECT_PIPE_NAME_LENGTH - 1;
			if (ap_len >= ACCESS_PATH_LENGTH)
				ap_len = ACCESS_PATH_LENGTH - 1;

			n = read_all(conn_req_fd, response_pipe_name, rpn_len);
			if (n != (ssize_t)rpn_len)
				break;
			response_pipe_name[rpn_len] = '\0';

			n = read_all(conn_req_fd, access_path, ap_len);
			if (n != (ssize_t)ap_len)
				break;
			access_path[ap_len] = '\0';

			/* Create worker thread to handle this connection */
			ConnectWorkerArg *work = malloc(sizeof(ConnectWorkerArg));

			if (!work)
				continue;

			strncpy(work->response_pipe_name, response_pipe_name,
				CONNECT_PIPE_NAME_LENGTH - 1);
			work->response_pipe_name[CONNECT_PIPE_NAME_LENGTH - 1] = '\0';
			strncpy(work->access_path, access_path,
				ACCESS_PATH_LENGTH - 1);
			work->access_path[ACCESS_PATH_LENGTH - 1] = '\0';

			pthread_t worker;

			if (pthread_create(&worker, NULL, connect_worker, work) != 0) {
				free(work);
				continue;
			}
		}

		close(conn_req_fd);
	}

	return NULL;
}

int main(void)
{
	/* Ensure directories exist */
	if (ensure_directory(DISPATCHER_DIR) == -1 ||
	    ensure_directory(PIPES_DIR) == -1) {
		return 1;
	}

	/* Clean up any existing pipes */
	cleanup_pipes();

	/* Create main dispatcher pipes */
	if (create_pipe(INSTALL_REQ_PIPE, NULL) == -1 ||
	    create_pipe(CONNECTION_REQ_PIPE, NULL) == -1) {
		return 1;
	}

	/* Create listener threads for install and connection requests */
	pthread_t install_thread, conn_thread;

	if (pthread_create(&install_thread, NULL, install_listener, NULL) != 0) {
		perror("pthread_create install_listener");
		return 1;
	}

	if (pthread_create(&conn_thread, NULL, connect_listener, NULL) != 0) {
		perror("pthread_create connect_listener");
		return 1;
	}

	/* Keep main thread alive */
	while (1)
		pause();

	return 0;
}
