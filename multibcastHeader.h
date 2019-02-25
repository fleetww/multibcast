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


typedef struct client {
	uint64_t pgid;
	uint16_t rank;
	struct sockaddr_in ip_addr;
}__attribute__ ((packed)) client; //packed because we send these across network

typedef struct clientList {
	client *head;
	struct clientList *tail;
} clientList;

static clientList *clientList(client *head, clientList *tail) {
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
} connection;
