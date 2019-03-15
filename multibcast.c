#include "multibcast.h"

int main(int argc, char **argv) {
	int op;
	int rank = -1, root = -1, size = -1;

	while ((op = getopt(argc, argv, "rns")) != -1) {
		switch (op) {
			case 'r':
				root = atoi(optarg);
				break;
			case 'n':
				rank = atoi(optarg);
				break;
			case 's':
				size = atoi(optarg);
				break;
		}
	}

	if (size <= 0) {
		printf("Must give a positive size for the group\n");
		exit(1);
	}

	if (rank < 0 || rank >= size) {
		printf("Must give a valid rank between [0, %d)\n", size);
		exit(1);
	}

	if (root < 0 || rank >= size) {
		printf("Must give a valid root between [0, %d)\n", size);
		exit(1);
	}

	TEST_Z(event_channel = rdma_create_event_channel());

	if (!rank) {//default coordinator
		become_coord();
	} else {
		init_coord_connection();
	}

	struct rdma_cm_event *event;
	while (rdma_get_cm_event(event_channel, &event) == 0) {
		struct rdma_cm_event event_copy;

		memcpy(&event_copy, event, sizeof(*event));
		rdma_ack_cm_event(event);

		if (on_event(&event_copy))
			break;
	}

	return 0;
}

int on_event(struct rdma_cm_event *event) {
	int r = 0;
	struct rdma_cm_id *id = event->id;

	switch (event->event) {
		case RDMA_CM_EVENT_ADDR_RESOLVED:
			r = on_addr_resolved(id);
			break;
		case RDMA_CM_EVENT_ROUTE_RESOLVED:
			r = on_route_resolved(id);
			break;
		case RDMA_CM_EVENT_CONNECT_REQUEST:
			r = on_connect_request(id);
			break;
		case RDMA_CM_EVENT_ESTABLISHED:
			r = on_connection(id);
			break;
		case RDMA_CM_EVENT_DISCONNECTED:
			r = on_disconnect(id);
			break;
		default:
			die("on_event: unknown event");
	}

	return r;
}

//Only called during connection process to coordinator?
int on_addr_resolved(struct rdma_cm_id *id) {
	struct ibv_qp_init_attr qp_attr;
	connection *conn;

	printf("address resolved");

	build_context(id->verbs);
	build_qp_attr(&qp_attr);

	TEST_NZ(rdma_create_qp(id, coord_ctx->pd, &qp_attr));

	conn = (connection *) calloc(1, sizeof(connection));
	id->context = conn;
	conn->cm_id = id;
	conn->qp = id->qp;

	register_memory(conn);

	return 0;
}

void build_context(struct ibv_context *verbs) {
	if (coord_ctx) {
		if (coord_ctx->ibv_ctx != verbs) {
			die("cannot handle events in more than one context");
		}
		return;
	}

	coord_ctx = (struct coord_context *) malloc(sizeof(struct coord_context));

	coord_ctx->ibv_ctx = verbs;

	TEST_Z(coord_ctx->pd = ibv_alloc_pd(coord_ctx->ibv_ctx));
	TEST_Z(coord_ctx->comp_channel = ibv_create_comp_channel(coord_ctx->ibv_ctx));
	TEST_Z(coord_ctx->cq = ibv_create_cq(coord_ctx->ibv_ctx, 10, NULL, coord_ctx->comp_channel, 0)); /*cqe=10 is arbitrary*/
	TEST_NZ(ibv_req_notify_cq(coord_ctx->cq, 0));

	TEST_NZ(pthread_create(&coord_ctx->cq_poller_thread, NULL, poll_cq_fn, NULL));
}

void build_qp_attr(struct ibv_qp_init_attr *qp_attr) {
	memset(qp_attr, 0, sizeof(*qp_attr));

	qp_attr->send_cq = coord_ctx->cq;
	qp_attr->recv_cq = coord_ctx->cq;
	qp_attr->qp_type = IBV_QPT_RC;

	qp_attr->cap.max_send_wr = 10; //arbitrary?
	qp_attr->cap.max_recv_wr = 10;
	qp_attr->cap.max_send_sge = 1;
	qp_attr->cap.max_recv_sge = 1;
}

void register_memory(connection *conn) {
	conn->clientList_size_buff = malloc(sizeof(uint64_t));
	TEST_Z(conn->clientList_size_mr = ibv_reg_mr(
				coord_ctx->pd,
				conn->clientList_size_buff,
				sizeof(uint64_t),
				IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));

	conn->sync_buff = malloc(sizeof(uint8_t));
	TEST_Z(conn->sync_mr = ibv_reg_mr(
				coord_ctx->pd,
				conn->sync_buff,
				sizeof(uint8_t),
				IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));
}

int on_route_resolved(struct rdma_cm_id *id) {
	struct rdma_conn_param cm_params;


	memset(&cm_params, 0, sizeof(struct rdma_conn_param));
	TEST_NZ(rdma_connect(id, &cm_params));

	printf("route resolved and now connecting\n");
	return 0;
}


bool first = true;
int on_connect_request(struct rdma_cm_id *id) {
	struct ibv_qp_init_attr qp_attr;
	struct rdma_conn_param cm_params;

	struct sockaddr_in * addr_in = (struct sockaddr_in *) rdma_get_peer_addr(id);
	char ipstr[INET_ADDRSTRLEN];
	inet_ntop(AF_INET, &addr_in->sin_addr, ipstr, INET_ADDRSTRLEN);
	uint16_t port = ntohs(rdma_get_dst_port(id));

	printf("received connection request from %s:%hu\n", ipstr, port);

	if (first) {
		client *c = (client *) malloc(sizeof(client));
		myclient = (client *) malloc(sizeof(client));

		uint64_t if_id1 = id->route.addr.addr.ibaddr.sgid.global.interface_id;//the worst reference ever! lol
		c->pgid = if_id1;
		c->cnum = htons(comm_no++);
		c->ip_addr = id->route.addr.src_sin;

		memcpy(&myclient->pgid, &c->pgid,sizeof(uint64_t));
		memcpy(&myclient->cnum, &c->cnum,sizeof(uint16_t));
		memcpy(&myclient->ip_addr, &c->ip_addr, sizeof(struct sockaddr_in));

		clients = ClientList(c, NULL);

		first = false;
	}

	client *cl = (client *) malloc(sizeof(client));
	cl->pgid = id->route.addr.addr.ibaddr.dgid.global.interface_id;
	cl->cnum = htons(comm_no);
	cl->ip_addr = id->route.addr.dst_sin;

	return 0;
}

int on_connection(struct rdma_cm_id *id) {
	struct sockaddr_in * addr_in = (struct sockaddr_in *) rdma_get_peer_addr(id);

	char ipstr[INET_ADDRSTRLEN];
	inet_ntop(AF_INET, &addr_in->sin_addr, ipstr, INET_ADDRSTRLEN);
	uint16_t port = ntohs(rdma_get_dst_port(id));

	printf("connected to %s:%hu\n", ipstr, port);

	return 0;
}

int on_disconnect(struct rdma_cm_id *id) {
	connection *conn = (connection *) id->context;

	rdma_destroy_qp(id);

	if (conn->clientList_size_buff) {
		ibv_dereg_mr(conn->clientList_size_mr);
		free(conn->clientList_size_buff);
	}
	if (conn->clientList_buff) {
		ibv_dereg_mr(conn->clientList_mr);
		free(conn->clientList_buff);
	}
	if (conn->sync_buff) {
		ibv_dereg_mr(conn->sync_mr);
		free(conn->sync_buff);
	}

	free(conn);

	rdma_destroy_id(id);

	return 1;
}

//polls cqe from coord comp_channel, handles when communications happen
void * poll_cq_fn(void *arg) {
	void *cq_context;//not used but function calls for it
	struct ibv_cq *cq;
	struct ibv_wc wc;

	while (1) {
		TEST_NZ(ibv_get_cq_event(coord_ctx->comp_channel, &cq, &cq_context));
		ibv_ack_cq_events(cq, 1);
		TEST_NZ(ibv_req_notify_cq(cq, 0)); //request notification for next CQE

		while (ibv_poll_cq(cq, 1, &wc)) {
			on_completion(&wc);
		}
	}
}

void on_completion(struct ibv_wc *wc) {
//TODO
}

void become_coord() {
	struct sockaddr_in addr;
	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_port = htons(atoi(DEF_COORD_PORT));

	TEST_NZ(rdma_create_id(event_channel, &coord_listener_id, NULL, RDMA_PS_TCP));
	TEST_NZ(rdma_bind_addr(coord_listener_id, (struct sockaddr *)&addr));
	TEST_NZ(rdma_listen(coord_listener_id, 10)); /* backlog=10 is arbitrary */
	uint16_t port = ntohs(rdma_get_src_port(coord_listener_id));
	struct sockaddr_in *localaddr_in =
		(struct sockaddr_in *) rdma_get_local_addr(coord_listener_id);
	char ipstr[INET_ADDRSTRLEN];
	inet_ntop(AF_INET, &localaddr_in->sin_addr, ipstr, INET_ADDRSTRLEN);

	am_coord = true;
	printf("I am become coordinator, worker of worlds!\n");
	printf("listening on %s:%hu\n", ipstr, port);
}

void init_coord_connection() {
	struct addrinfo *addr;
	TEST_NZ(getaddrinfo(DEF_COORD_ADDR, DEF_COORD_PORT, NULL, &addr));

	TEST_NZ(rdma_create_id(event_channel, &coord_connect_id, NULL, RDMA_PS_TCP));
	TEST_NZ(rdma_resolve_addr(coord_connect_id, NULL, addr->ai_addr, TIMEOUT_IN_MS));
	printf("resolving address to coordinator at %s:%s\n", DEF_COORD_ADDR, DEF_COORD_PORT);

	freeaddrinfo(addr);
}

int get_completion(struct ibv_cq *cq) {
	int ret;
	struct ibv_wc wc;
	do {
		ret = ibv_poll_cq(cq, 1, &wc);
		if (ret < 0) {
			VERB_ERR("ibv_poll_cq", ret);
			return -1;
		}
	}
	while (ret == 0);
	if (wc.status != IBV_WC_SUCCESS) {
		printf("work completion status %s\n",
				ibv_wc_status_str(wc.status));
		return -1;
	}
	return 0;
}

void die(const char *reason) {
	fprintf(stderr, "%s\n", reason);
	exit(EXIT_FAILURE);
}
