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
	post_client_list_size_recv(conn);

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

	conn->sync_buff = malloc(sizeof(uint16_t));
	TEST_Z(conn->sync_mr = ibv_reg_mr(
				coord_ctx->pd,
				conn->sync_buff,
				sizeof(uint16_t),
				IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));
}

void post_client_list_size_recv(connection *conn) {
	struct ibv_recv_wr wr, *bad_wr = NULL;
	struct ibv_sge sge;

	wr.wr_id = (uintptr_t) conn;
	wr.next = NULL;
	wr.sg_list = &sge;
	wr.num_sge = 1;

	sge.addr = (uintptr_t) conn->clientList_size_buff;
	sge.length = sizeof(uint64_t);
	sge.lkey = conn->clientList_size_mr->lkey;

	TEST_NZ(ibv_post_recv(conn->qp, &wr, &bad_wr));
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
	numm = 0;

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
		c->rank = htons(comm_no++); //comm_no starts at 0
		c->ip_addr = id->route.addr.src_sin;

		memcpy(&myclient->pgid, &c->pgid,sizeof(uint64_t));
		memcpy(&myclient->cnum, &c->cnum,sizeof(uint16_t));
		memcpy(&myclient->ip_addr, &c->ip_addr, sizeof(struct sockaddr_in));

		clients = ClientList(c, NULL);

		first = false;
	}

	client *cl = (client *) malloc(sizeof(client));
	cl->pgid = id->route.addr.addr.ibaddr.dgid.global.interface_id;
	cl->rank = htons(comm_no++);
	cl->ip_addr = id->route.addr.dst_sin;

	build_context(id->verbs);
	struct ibv_qp_init_attr qp_attr;
	build_qp_attr(&qp_attr);

	TEST_NZ(rdma_create_qp(id, coord_ctx->pd, &qp_attr));
	connection conn = (connection *) malloc(sizeof(connection));
	id->context = conn;
	conn->cm_id = id;
	conn->qp = id->qp;
	conn->conn_id = comm_no;
	conn->send_state = SS_INIT;
	conn->recv_state = RS_INIT;

	uint16_t cl_size = comm_no * sizeof(client);
	if (comm_no == 2) {//first connection made
		conn->clientList_size_buff = malloc(sizeof(uint16_t));
		TEST_Z(conn->clientList_size_mr = ibv_reg_mr(
					coord_ctx->pd,
					conn->clientList_size_buff,
					sizeof(uint16_t),
					IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));

		conn->clientList_buff = malloc(cl_size);
		TEST_Z(conn->clientList_buff_mr = ibv_reg_mr(
					coord_ctx->pd,
					conn->clientList_buff,
					cl_size,
					IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_LOCAL_WRITE));

	conn->sync_buff = malloc(sizeof(uint16_t));
	TEST_Z(conn->sync_mr = ibv_reg_mr(
				coord_ctx->pd,
				conn->sync_buff,
				sizeof(uint16_t),
				IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));
	} else {
		conn->clientList_buff = realloc(clientList_buff, cl_size);

		ibv_dereg_mr(conn->clientList_buff_mr);

		TEST_Z(conn->clientList_buff_mr = ibv_reg_mr(
					coord_ctx->pd,
					conn->clientList_buff,
					cl_size,
					IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_LOCAL_WRITE));
	}

	clients = ClientList(cl, clients);
	int i = 0;
	for (clientList *clt = clients; clt; clt = clt->tail) {
		client cc = clt->head;
		memcpy(clientList_buff + (i * sizeof(client)), cc, sizeof(client));
		i++;
	}

	connections = ConnectionList(conn, connections);
	*(conn->clientList_size_buff) = htons(comm_no);
	printmultiple(conn);
	post_sync_msg_receives();

	struct rdma_conn_param cm_params;
	memset(&cm_params, 0, sizeof(rdma_conn_param));

	TEST_NZ(rdma_accept(id, &cm_params));

	return 0;
}

void post_sync_msg_receives() {
	for (connectionList *clt = connections; clt; clt = clt->tail) {
		connection *conn = clt->head;
		struct ibv_recv_wr wr, *bad_wr = NULL;
		struct ibv_sge sge;

		wr.wr_id = (uintptr_t) conn;
		wr.next = NULL;
		wr.sg_list = &sge;
		wr.num_sge = 1;

		sge.addr = (uintptr_t) conn->sync_buff;
		sge.length = sizeof(uint16_t);
		sge.lkey = conn->sync_mr->lkey;

		TEST_NZ(ibv_post_recv(conn->qp, &wr, &bad_wr));
	}
}

void printmultiple(connection *conn) {
	char str[INET_ADDRSTRLEN];
	printf("Client List:\n");
	uint16_t num_clients = ntohs(*(conn->clientList_size_buff));

	for (uint16_t i = 0; i < num_clients; i++) {
		client *ct = conn->clientList_buff + (i * sizeof(client));
		inet_ntop(AF_INET, &(ct->ip_addr.sin_addr), str, INET_ADDRSTRLEN);
		printf("\tClient pgid = %"PRIx64"\n", be64toh(ct->pgid));
		printf("\tClient rank = 0x%x\n", ntohs(ct->rank));
		printf("\tClient IP = %s\n", str);
		if (i < num_clients-1) printf("\n");
	}
}

int on_connection(struct rdma_cm_id *id) {
	connection *conn = (connection *) id->context;

	struct sockaddr_in * addr_in = (struct sockaddr_in *) rdma_get_peer_addr(id);
	char ipstr[INET_ADDRSTRLEN];
	inet_ntop(AF_INET, &addr_in->sin_addr, ipstr, INET_ADDRSTRLEN);
	uint16_t port = ntohs(rdma_get_dst_port(id));
	printf("connected to %s:%hu\n", ipstr, port);

	if (am_coord) {
		printf("Posting client list size = %hd\n", comm_no);
		post_client_list_size_send(conn);
	} else {
		mygid = be64toh(conn->id->route.addr.addr.ibaddr.sgid.global.interface_id);
		printf("mygid = %"PRIx64"\n", mygid);
	}
	return 0;
}

void post_client_list_size_send() {
	for (connectionList *clt = connections; clt; clt = clt->tail) {
		connection *conn = clt->head;

		struct ibv_send_wr wr, *bad_wr = NULL;
		struct ibv_sge sge;
		memset(&wr, 0, sizeof(struct ibv_send_wr));
		wr.wr_id = (uintptr_t) conn;
		wr.opcode = IBV_WR_SEND;
		wr.sg_list = &sge;
		wr.num_sge = 1;
		wr.send_flags = IBV_SEND_SIGNALED;

		sge.addr = (uintptr_t) conn->clientList_size_buff;
		sge.length = sizeof(uint16_t);
		sge.lkey = conn->clientList_size_mr->lkey;

		TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));
	}
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

	return 1; //should we try to recover?
}

//polls cqe from coord comp_channel, handles when communications happen
void *poll_cq_fn(void *arg) {
	void *cq_context;//not used but function calls for it
	struct ibv_cq *cq;
	struct ibv_wc wc;

	while (1) {
		TEST_NZ(ibv_get_cq_event(coord_ctx->comp_channel, &cq, &cq_context));
		ibv_ack_cq_events(cq, 1);
		TEST_NZ(ibv_req_notify_cq(cq, 0)); //request notification for next CQE

		int status = 0;
		while (ibv_poll_cq(cq, 1, &wc)) {
			status = on_completion(&wc);
		}

		if (status) break;
	}

	return NULL;
}

int on_completion(struct ibv_wc *wc) {
	if (wc->status != IBV_WC_SUCCESS) {
		printf("Work Completion FAILED, vendor error: %d, status: %d\n",
				wc->vendor_err, wc->status);
		die("on_completion: status is not IBV_WC_SUCCESS");
	}

	int status = 0;

	switch (wc->opcode) {
		case IBV_WC_SEND:
			printf("Send successfull\n");
			break;
		case IBV_WC_RECV:
			if (am_coord) {
				status = coord_recv_completion(wc);
			} else {
				status = client_recv_completion(wc);
			}
			break;
	}

	return status;
}

int coord_recv_completion(struct ibv_wc *wc) {
	connection *conn = (connection *) wc->wr_id;

	printf("received sync msg\n");

	if (conn-recv_state == RS_INIT) {
		numm++;
		printf("sending client list, num clients: %hd\n", comm_no);
		post_client_list_send(conn);

		if ((comm_no == MAX_CLIENTS) && ((numm + 1) == MAX_CLIENTS)) {
			printf("All clients connected, starting mcast()\n");
			mcast();
			return 1;
		}
	}

	return 0;
}

void post_client_list_send(connection *conn) {
	struct ibv_send_wr wr, *bad_wr = NULL;
	struct ibv_sge sge;
	memset(&wr, 0, sizeof(struct ibv_send_wr));
	wr.wr_id = (uintptr_t) conn;
	wr.opcode = IBV_WR_SEND;
	wr.sg_list = &sge;
	wr.nume_sge = 1;
	wr.send_flags = IBV_SEND_SIGNALED;

	sge.addr = (uintptr_t) conn->clientList_buff;
	sge.length = comm_no * sizeof(client);
	sge.lkey = conn->clientList_mr->lkey;

	TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));
}

int client_recv_completion(struct ibv_wc *wc) {
	connection *conn = (connection *) wc->wr_id;

	if (conn->recv_state == RS_INIT) {

	}

	return 0;
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

	am_coord = false;
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
