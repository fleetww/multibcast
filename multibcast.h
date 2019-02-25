#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <rdma/rdma_cma.h>

#define DEF_COORD_ADDR "192.168.143.141"
#define DEF_COORD_PORT "20079" //rdma ops use strings to specify port
#define MULTICAST_ADDR "239.0.0.1"
#define MULTICAST_PORT "51216"
#define ACK_PORT "65079"

#define DEFAULT_MSG_COUNT 1
#define DEFAULT_MSG_LENGTH 2048
#define MAX_CLIENTS 4

#define BUFFER_SIZE = 1024;

#define VERB_ERR(verb, ret) \
fprintf(stderr, "%s returned %d errno %d\n", verb, ret, errno)

typedef struct client {
	uint64_t pgid;
	uint16_t rank;
	struct sockaddr_in ip_addr;
}__attribute__ ((packed)) client; //packed because we send these across network

typedef struct clientList {
	client *head;
	struct clientList *tail;
} clientList;

clientList *ClientList(client *head, clientList *tail) {
	clientList *cl = calloc(1, sizeof(clientList));
	cl->head = head;
	cl->tail = tail;
	return cl;
}

//used for coordinator communication
typedef struct connection {
	uint16_t conn_id;

	struct rdma_cm_id *cm_id;
	struct ibv_qp *qp;

	char *clientList_buff;
	struct ibv_mr *clientList_mr;

	char *sync_buff;
	struct ibv_mr *sync_mr;
} connection;

typedef struct connectionList {
	connection *head;
	struct connectionList *tail;
} connectionList;

connectionList *ConnectionList(connection *head, connectionList *tail) {
	connectionList *cl = calloc(1, sizeof(connectionList));
	cl->head = head;
	cl->tail = tail;
	return cl;
}

struct mcontext {
	/* User parameters */
	int sender;
	char *bind_addr;
	char *mcast_addr;
	char *server_port;
	int msg_count;
	int msg_length;
	/* Resources */
	struct sockaddr mcast_sockaddr;
	struct rdma_cm_id *id;
	struct rdma_event_channel *channel;
	struct ibv_pd *pd;
	struct ibv_cq *cq;
	struct ibv_mr *mr;
	char *buf;
	struct ibv_ah *ah;
	uint32_t remote_qpn;
	uint32_t remote_qkey;
	pthread_t cm_thread;
};


int mcast();

int resolve_addr(struct mcontext *ctx);

int on_event(struct rdma_cm_event *event);
int on_addr_resolved(struct rdma_cm_id *id)
int on_route_resolved(struct rdma_cm_id *id);
int on_connect_request(struct rdma_cm_id *id);
int on_connection(void *context);
int on_disconnect(struct rdma_cm_id *id);

int get_completion(struct ibv_cq *cq);
void on_completion(struct ibv_wc *wc);

