#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdbool.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
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

#define BUFFER_SIZE 1024
#define TIMEOUT_IN_MS 500

#define VERB_ERR(verb, ret) \
fprintf(stderr, "%s returned %d errno %d\n", verb, ret, errno)

#define TEST_NZ(x) do { if ( (x)) die("error: " #x " failed (returned non-zero)." ); } while (0)
#define TEST_Z(x)  do { if (!(x)) die("error: " #x " failed (returned zero/null)."); } while (0)

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

clientList *clients = NULL;

client *myclient = NULL:
uint64_t mygid;
uint16_t comm_no = 0;
uint32_t numm=0;

struct rdma_event_channel *event_channel;

struct rdma_cm_id *coord_connect_id = NULL;
struct rdma_cm_id *coord_listener_id = NULL;

bool am_coord = false;

//manages ibv resoruces for coordinator comms
struct coord_context {
	struct ibv_context *ibv_ctx;
	struct ibv_pd *pd;
	struct ibv_cq *cq;
	struct ibv_comp_channel *comp_channel;

	pthread_t cq_poller_thread;
};

struct coord_context *coord_ctx;

//used for coordinator communication
typedef struct connection {
	uint16_t conn_id;

	struct rdma_cm_id *cm_id;
	struct ibv_qp *qp;

	//we will replace this with a ringed memory buffer
	uint64_t *clientList_size_buff;
	struct ibv_mr *clientList_size_mr;

	char *clientList_buff;
	struct ibv_mr *clientList_mr;

	uint16_t *sync_buff;
	struct ibv_mr *sync_mr;

	enum {
		RS_INIT,
		RS_SIZE,
		RS_LIST
	} recv_state;
	enum {
		SS_INIT,
		SS_SIZE,
		SS_LIST
	} send_state;
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

connectionList *connections = NULL;

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
int on_addr_resolved(struct rdma_cm_id *id);
int on_route_resolved(struct rdma_cm_id *id);
int on_connect_request(struct rdma_cm_id *id);
int on_connection(struct rdma_cm_id *id);
int on_disconnect(struct rdma_cm_id *id);

int get_completion(struct ibv_cq *cq);
void on_completion(struct ibv_wc *wc);
void coord_recv_completion(struct ibv_wc *wc);
void client_recv_completion(struct ibv_wc *wc);

void *poll_cq_fn(void *arg);

void build_context(struct ibv_context *verbs);
void build_qp_attr(struct ibv_qp_init_attr *qp_attr);
void register_memory(connection *conn);
void printmultiple(connection *conn);
void post_receives();
void post_client_list_size_send();
void post_client_list_send(connection *conn);

void mcast();

void become_coord();
void init_coord_connection();
void die(const char *reason);
