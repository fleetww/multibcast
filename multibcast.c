#include "multibcast.h"

int main(int argc, char **argv) {
	int op;
	char *bind_addr = NULL;

	while ((op = getopt(argc, argv, "r:n:s:b:")) != -1) {
		switch (op) {
			case 'r':
				ROOT = atoi(optarg);
				break;
			case 'n':
				RANK = atoi(optarg);
				break;
			case 's':
				MAX_CLIENTS = atoi(optarg);
				break;
			case 'b':
				bind_addr = strdup(optarg);
				break;
		}
	}

	if (MAX_CLIENTS == 0) {
		printf("Must give a positive size for the number of clients\n");
		exit(1);
	}

	if (RANK < 0 || RANK >= MAX_CLIENTS) {
		printf("Must give a valid rank between [0, %d)\n", MAX_CLIENTS);
		exit(1);
	}

	if (ROOT < 0 || RANK >= MAX_CLIENTS) {
		printf("Must give a valid root between [0, %d)\n", MAX_CLIENTS);
		exit(1);
	}

	if (!RANK) {//default coordinator
		become_coord();
	} else {
		init_coord_connection(bind_addr);
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

void become_coord() {
	struct sockaddr_in addr_in_coord;
	memset(&addr_in_coord, 0, sizeof(addr_in_coord));
	addr_in_coord.sin_family = AF_INET;
	addr_in_coord.sin_port = htons((uint16_t) atoi(DEF_COORD_PORT));

	TEST_Z(event_channel = rdma_create_event_channel());

	TEST_NZ(rdma_create_id(event_channel, &coord_listener_id, NULL, RDMA_PS_TCP));
	TEST_NZ(rdma_bind_addr(coord_listener_id, (struct sockaddr *)&addr_in_coord));
	TEST_NZ(rdma_listen(coord_listener_id, 10)); // backlog=10 is arbitrary

	uint16_t port = ntohs(rdma_get_src_port(coord_listener_id));
	struct sockaddr_in *localaddr_in =
		(struct sockaddr_in *) rdma_get_local_addr(coord_listener_id);
	char ipstr[INET_ADDRSTRLEN];
	inet_ntop(AF_INET, &localaddr_in->sin_addr, ipstr, INET_ADDRSTRLEN);

	printf("I am become coordinator, worker of worlds!\n");
	printf("listening on %s:%hu\n", ipstr, port);

	am_coord = true;
}

void init_coord_connection(char *bind_addr) {
	struct addrinfo *addrinf;
	TEST_NZ(getaddrinfo(DEF_COORD_ADDR, DEF_COORD_PORT, NULL, &addrinf));

	TEST_Z(event_channel = rdma_create_event_channel());
	TEST_NZ(rdma_create_id(event_channel, &coord_connect_id, NULL, RDMA_PS_TCP));
	TEST_NZ(rdma_resolve_addr(coord_connect_id, NULL, addrinf->ai_addr, TIMEOUT_IN_MS));

	printf("resolving address to coordinator at %s:%s\n", DEF_COORD_ADDR, DEF_COORD_PORT);
	freeaddrinfo(addrinf);

	am_coord = false;
}

int on_event(struct rdma_cm_event *event) {
	int r = 0;
	struct rdma_cm_id *id = event->id;

	switch (event->event) {
		case RDMA_CM_EVENT_ADDR_RESOLVED:
			r = on_addr_resolved(id);
			break;
		case RDMA_CM_EVENT_ADDR_ERROR:
			printf("error resolving address\n");
			r = 1;
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

	printf("address resolved\n");

	build_context(id->verbs);
	build_qp_attr(&qp_attr);

	TEST_NZ(rdma_create_qp(id, coord_ctx->pd, &qp_attr));

	conn = (connection *) calloc(1, sizeof(connection));
	id->context = conn;
	conn->cm_id = id;
	conn->qp = id->qp;

	printf("Registering size and sync memory regions\n");

	if (!clientList_size_mr)
		TEST_Z(clientList_size_mr = ibv_reg_mr(
					coord_ctx->pd,
					&clientList_size_buff,
					sizeof(uint32_t),
					IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));

	if (!sync_mr)
		TEST_Z(sync_mr = ibv_reg_mr(
					coord_ctx->pd,
					&sync_buff,
					sizeof(uint16_t),
					IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));

	post_client_list_size_recv(conn);

	TEST_NZ(rdma_resolve_route(id, TIMEOUT_IN_MS));

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
	printf("context built\n");
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

	printf("qp attributes built\n");
}

//TODO Not needed anymore?
/*
void register_memory(connection *conn) {
	conn->clientList_size_buff = malloc(sizeof(uint32_t));
	TEST_Z(conn->clientList_size_mr = ibv_reg_mr(
				coord_ctx->pd,
				conn->clientList_size_buff,
				sizeof(uint32_t),
				IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));

	conn->sync_buff = malloc(sizeof(uint16_t));
	TEST_Z(conn->sync_mr = ibv_reg_mr(
				coord_ctx->pd,
				conn->sync_buff,
				sizeof(uint16_t),
				IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));

	printf("memory registered\n");
}
*/

void post_client_list_size_recv(connection *conn) {
	struct ibv_recv_wr wr, *bad_wr = NULL;
	struct ibv_sge sge;

	wr.wr_id = (uintptr_t) conn;
	wr.next = NULL;
	wr.sg_list = &sge;
	wr.num_sge = 1;

	sge.addr = (uintptr_t) &clientList_size_buff;
	sge.length = sizeof(uint32_t);
	sge.lkey = clientList_size_mr->lkey;

	TEST_NZ(ibv_post_recv(conn->qp, &wr, &bad_wr));

	printf("posted client list size receive\n");
}

void post_client_list_recv(connection *conn) {
	struct ibv_recv_wr wr, *bad_wr = NULL;
	struct ibv_sge sge;
	wr.wr_id = (uintptr_t)conn;
	wr.next = NULL;
	wr.sg_list = &sge;
	wr.num_sge = 1;

	sge.addr = (uintptr_t)clientList_buff;
	sge.length = ntohl(clientList_size_buff) * sizeof(client);
	sge.lkey = clientList_mr->lkey;

	TEST_NZ(ibv_post_recv(conn->qp, &wr, &bad_wr));

	printf("posted client list receive\n");
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
		//Build up server's client structure, before we handle client's
		client *c = (client *) malloc(sizeof(client));
		myclient = (client *) malloc(sizeof(client));

		uint64_t if_id1 = id->route.addr.addr.ibaddr.sgid.global.interface_id;//the worst reference ever! lol
		c->pgid = if_id1;
		c->rank = htons(comm_no++); //comm_no starts at 0
		c->ip_addr = id->route.addr.src_sin;

		memcpy(&myclient->pgid, &c->pgid, sizeof(uint64_t));
		memcpy(&myclient->rank, &c->rank, sizeof(uint16_t));
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
	connection *conn = (connection *) malloc(sizeof(connection));
	id->context = conn;
	conn->cm_id = id;
	conn->qp = id->qp;
	conn->conn_id = comm_no;
	conn->send_state = SS_INIT;
	conn->recv_state = RS_INIT;

	printf("comm_no: %d\n", comm_no);

	if (!clientList_size_mr)
		TEST_Z(clientList_size_mr = ibv_reg_mr(
					coord_ctx->pd,
					&clientList_size_buff,
					sizeof(uint32_t),
					IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));

	uint32_t cl_size = comm_no * sizeof(client);
	clientList_buff = (char *) realloc(clientList_buff, cl_size);

	if (clientList_mr)
		ibv_dereg_mr(clientList_mr);
	TEST_Z(clientList_mr = ibv_reg_mr(
				coord_ctx->pd,
				clientList_buff,
				cl_size,
				IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));

	if (!sync_mr)
		TEST_Z(sync_mr = ibv_reg_mr(
					coord_ctx->pd,
					&sync_buff,
					sizeof(uint16_t),
					IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));

	clients = ClientList(cl, clients);
	int i = 0;
	for (clientList *clt = clients; clt; clt = clt->tail) {
		client *cc = clt->head;
		memcpy(clientList_buff + (i * sizeof(client)), cc, sizeof(client));
		i++;
	}

	connections = ConnectionList(conn, connections);
	clientList_size_buff = htonl(comm_no);
	print_client_list();
	post_sync_msg_receives();

	struct rdma_conn_param cm_params;
	memset(&cm_params, 0, sizeof(struct rdma_conn_param));

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

		sge.addr = (uintptr_t) &sync_buff;
		sge.length = sizeof(uint16_t);
		sge.lkey = sync_mr->lkey;

		TEST_NZ(ibv_post_recv(conn->qp, &wr, &bad_wr));
	}
}

void post_sync_msg_send(connection *conn) {
	struct ibv_send_wr wr, *bad_wr = NULL;
	struct ibv_sge sge;

	sync_buff = htons(1);

	memset(&wr, 0, sizeof(struct ibv_send_wr));
	wr.wr_id = (uintptr_t) conn;
	wr.opcode = IBV_WR_SEND;
	wr.sg_list = &sge;
	wr.num_sge = 1;
	wr.send_flags = IBV_SEND_SIGNALED;

	sge.addr = (uintptr_t)&sync_buff;
	sge.length = sizeof(uint16_t);
	sge.lkey = sync_mr->lkey;

	TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));

	printf("posting sync msg send\n");
}

void print_client_list() {
	char str[INET_ADDRSTRLEN];
	printf("Client List:\n");
	uint32_t num_clients = ntohl(clientList_size_buff);

	for (uint32_t i = 0; i < num_clients; i++) {
		client *ct = (client *)(clientList_buff + (i * sizeof(client)));
		inet_ntop(AF_INET, &(ct->ip_addr.sin_addr), str, INET_ADDRSTRLEN);

		printf("  Client pgid = %"PRIx64"\n", be64toh(ct->pgid));
		printf("  Client rank = 0x%x\n", ntohs(ct->rank));
		printf("  Client IP = %s\n\n", str);
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
		post_client_list_size_send(conn);
	} else {
		mygid = be64toh(conn->cm_id->route.addr.addr.ibaddr.sgid.global.interface_id);
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

		sge.addr = (uintptr_t) &clientList_size_buff;
		sge.length = sizeof(uint32_t);
		sge.lkey = clientList_size_mr->lkey;

		TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));
	}
		printf("Posting client list size = %hd sends\n", comm_no);
}

int on_disconnect(struct rdma_cm_id *id) {
	connection *conn = (connection *) id->context;

	rdma_destroy_qp(id);

	if (clientList_size_mr) {
		ibv_dereg_mr(clientList_size_mr);
	}
	if (clientList_buff) {
		ibv_dereg_mr(clientList_mr);
		free(clientList_buff);
	}
	if (sync_mr) {
		ibv_dereg_mr(sync_mr);
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
			if (status) break;
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
		default:
			die("on_completion: completion isn't a send or a receive");
	}

	return status;
}


void multicast() {
	printf("Starting multicast test\n");

	struct mcontext mtx;
	memset(&mtx, 0, sizeof(struct mcontext));
	mtx.sender = ROOT;
	mtx.bind_addr = "0.0.0.0";
	mtx.mcast_addr = MULTICAST_ADDR;
	mtx.server_port = MULTICAST_PORT;
	mtx.msg_count = DEFAULT_MSG_COUNT;
	mtx.msg_length = sizeof(struct message);
	TEST_Z(mtx.channel = rdma_create_event_channel());
	TEST_NZ(rdma_create_id(mtx.channel, &mtx.id, NULL, RDMA_PS_UDP));

	TEST_NZ(resolve_addr(&mtx));

	struct ibv_port_attr port_attr;
	TEST_NZ(ibv_query_port(mtx.id->verbs, mtx.id->port_num, &port_attr));
	int active_mtu = 1 << (port_attr.active_mtu + 7);
	if (mtx.msg_length > active_mtu) {
		printf("buffer length %d is larger than active mtu %d\n", mtx.msg_length, active_mtu);
		exit(EXIT_FAILURE);
	}

	create_resources(&mtx);

	if (mtx.sender != RANK) {
		for (int i = 0; i < mtx.msg_count; i++) {
			//rdma_post_recv(mtx.id, NULL, mtx.buff + i)
			TEST_NZ(rdma_post_recv(mtx.id,
						NULL,
						mtx.buff + (i * (mtx.msg_length + sizeof(struct ibv_grh))),
						mtx.msg_length + sizeof(struct ibv_grh),
						mtx.mr));
		}
	}

	TEST_NZ(rdma_join_multicast(mtx.id, &mtx.mcast_sockaddr, NULL));
	printf("Joined multicast group\n");
	struct rdma_cm_event *mevent;
	TEST_NZ(get_cm_event(mtx.channel, RDMA_CM_EVENT_MULTICAST_JOIN, &mevent));
	mtx.remote_qpn = mevent->param.ud.qp_num;
	mtx.remote_qkey = mevent->param.ud.qkey;

	if (mtx.sender == ROOT) {
		TEST_Z(mtx.ah = ibv_create_ah(mtx.pd, &mevent->param.ud.ah_attr));
	}
	rdma_ack_cm_event(mevent);

	pthread_create(&mtx.cm_thread, NULL, cm_thread, &mtx);

	if (mtx.sender != ROOT) {
		printf("Waiting for multicast messages...\n");
	}
	for (int i = 0; i < mtx.msg_count; i++) {
		if (mtx.sender == ROOT) {
			post_multicast_send(&mtx, i);
		}

		get_completion(mtx.cq);
		if (mtx.sender == ROOT) {
			printf("Sent multicast message %d\n", i);
		} else {
			printf("Received message %d\n", i);
		}

		multicast_ack_cast();
	}
}

int nextPowerOf2(uint16_t n){
	int p=1;
	while(p<n)
		p<<=1;

	return p;
}

void multicast_ack_cast() {
	printf("Inside multicast ack cast\n");

	bool isPow2 = (MAX_CLIENTS != 0) && ((MAX_CLIENTS & (MAX_CLIENTS - 1)) == 0);

	uint32_t loopcnt;
	if (isPow2)
		uint32_t loopcnt = (int) log2((double)MAX_CLIENTS);
	else
		loopcnt = (int)log2(nextPowerOf2(MAX_CLIENTS));

	for (int i = 0; i < loopcnt; i++) {
		if ((RANK & ((1 << (i+1)) - 1)) == 0)
			TEST_NZ(servmethod(isPow2, loopcnt, i));
		if ((RANK & ((1 << (i+1)) - 1)) == 1 << i)
			TEST_NZ(connectmethod(i));
	}
}

int post_multicast_send(mcontext *mtx, int msg_num) {
	struct ibv_send_wr wr, *bad_wr = NULL;
	struct ibv_sge sge;

	struct message msg;
	msg.msg_num = htons(msg_num);
	snprintf(msg.msg, MSG_STR_LENGTH, "message %d from server", msg_num);
	memcpy(mtx->buff, &msg, sizeof(struct message));

	memset(&wr, 0, sizeof(struct ibv_send_wr));
	wr.wr_id = 0;
	wr.opcode = IBV_WR_SEND_WITH_IMM;
	wr.sg_list = &sge;
	wr.num_sge = 1;
	wr.send_flags = IBV_SEND_SIGNALED;
	// Multicast requires that the message is sent with immediate data
	// and that the QP number is the contents of the immediate data
	wr.imm_data = htonl(mtx->id->qp->qp_num);
	wr.wr.ud.ah = mtx->ah;
	wr.wr.ud.remote_qpn = mtx->remote_qpn;
	wr.wr.ud.remote_qkey = mtx->remote_qkey;

	sge.length = mtx->msg_length;
	sge.lkey = mtx->mr->lkey;
	sge.addr = (uintptr_t) mtx->buff;

	printf("Sending message %d:\n\t\"%s\"\n", msg_num, msg.msg);
	TEST_NZ(ibv_post_send(mtx->id->qp, &wr, &bad_wr));

	return 0;
}

void *cm_thread(void *arg) {
	struct rdma_cm_event *event;
	mcontext *mtx = (mcontext *) arg;

	while (1) {
		TEST_NZ(rdma_get_cm_event(mtx->channel, &event));

		printf("event %s, status %d\n",
				rdma_event_str(event->event), event->status);

		rdma_ack_cm_event(event);
	}

	return NULL;
}

int create_resources(mcontext *mtx) {
	struct ibv_qp_init_attr attr;
	memset(&attr, 0, sizeof(struct ibv_qp_init_attr));
	if (!mtx->pd) {
		TEST_Z(mtx->pd = ibv_alloc_pd(mtx->id->verbs));
	}

	TEST_Z(mtx->cq = ibv_create_cq(mtx->id->verbs, 2, 0, 0, 0));

	attr.qp_type = IBV_QPT_UD;
	attr.send_cq = mtx->cq;
	attr.recv_cq = mtx->cq;
	attr.cap.max_send_wr = mtx->msg_count;
	attr.cap.max_recv_wr = mtx->msg_count;
	attr.cap.max_send_sge = 1;
	attr.cap.max_recv_sge = 1;
	TEST_NZ(rdma_create_qp(mtx->id, mtx->pd, &attr));

	//The receiver must allow enough space in the receive buffer for the GRH
	uint32_t msg_size = mtx->msg_length + ((mtx->sender == RANK) ? 0 : sizeof(struct ibv_grh));
	uint32_t buff_size = msg_size * mtx->msg_count;
	//uint32_t buff_size = DEFAULT_MSG_LENGTH + (mtx->sender == RANK)	? 0 : sizeof(struct ibv_grh);
	mtx->buff = calloc(0, buff_size);
	TEST_Z(mtx->mr = rdma_reg_msgs(mtx->id, mtx->buff, buff_size));

	return 0;
}

int resolve_addr(mcontext *mtx) {
	struct rdma_addrinfo *bind_rai = NULL, *mcast_rai = NULL, hints;

	memset(&hints, 0, sizeof (hints));
	hints.ai_port_space = RDMA_PS_UDP;
	if (mtx->bind_addr) {
		hints.ai_flags = RAI_PASSIVE;
		TEST_NZ(rdma_getaddrinfo(mtx->bind_addr, NULL, &hints, &bind_rai));
	}

	hints.ai_flags = 0;
	TEST_NZ(rdma_getaddrinfo(mtx->mcast_addr, NULL, &hints, &mcast_rai));

	if (mtx->bind_addr) {
		/* bind to a specific adapter if requested to do so */
		TEST_NZ(rdma_bind_addr(mtx->id, bind_rai->ai_src_addr));
		/* A PD is created when we bind. Copy it to the context so it can
		 * be used later on */
		mtx->pd = mtx->id->pd;
	}

	TEST_NZ(rdma_resolve_addr(
				mtx->id,
				(bind_rai) ? bind_rai->ai_src_addr : NULL,
				mcast_rai->ai_dst_addr,
				2000));

	TEST_NZ(get_cm_event(mtx->channel, RDMA_CM_EVENT_ADDR_RESOLVED, NULL));

	memcpy(&mtx->mcast_sockaddr,
			mcast_rai->ai_dst_addr,
			sizeof (struct sockaddr));

	return 0;
}

int get_cm_event(struct rdma_event_channel *channel,
		enum rdma_cm_event_type type,
		struct rdma_cm_event **out_ev) {
	int ret = 0;

	struct rdma_cm_event *event = NULL;
	ret = rdma_get_cm_event(channel, &event);
	if (ret) {
		VERB_ERR("rdma_get_cm_event", ret);
		return -1;
	}

	/* Verify the event is the expected type */
	if (event->event != type) {
		printf("get_cm_event: \tevent: %s, status: %d\n",
				rdma_event_str(event->event), event->status);
		ret = -1;
	}

	/* Pass the event back to the user if requested */
	if (!out_ev)
		rdma_ack_cm_event(event);
	else
		*out_ev = event;
	return ret;
}

int coord_recv_completion(struct ibv_wc *wc) {
	connection *conn = (connection *) wc->wr_id;

	printf("received sync msg\n");

	if (conn->recv_state == RS_INIT) {
		numm++;
		printf("sending client list, num clients: %hd\n", comm_no);
		post_client_list_send(conn);

		if ((comm_no == MAX_CLIENTS) && ((numm + 1) == MAX_CLIENTS)) {
			printf("All clients connected, starting multicast()\n");
			multicast();
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
	wr.num_sge = 1;
	wr.send_flags = IBV_SEND_SIGNALED;

	sge.addr = (uintptr_t) clientList_buff;
	sge.length = ntohl(clientList_size_buff) * sizeof(client);
	sge.lkey = clientList_mr->lkey;

	TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));
}

int client_recv_completion(struct ibv_wc *wc) {
	connection *conn = (connection *) wc->wr_id;

	if (conn->recv_state == RS_INIT) {
		printf("Received new num clients: %u\n", ntohl(clientList_size_buff));
		conn->recv_state = RS_SIZE;
		reg_client_list_mem(conn);
		post_client_list_recv(conn);
		post_sync_msg_send(conn);
	} else if (conn->recv_state == RS_SIZE) {
		printf("Received new client list\n");
		print_client_list();
		//mcast? if not restart process
		uint32_t num_clients = ntohl(clientList_size_buff);
		if (num_clients == MAX_CLIENTS) {
			multicast();
		} else {
			conn->recv_state = RS_INIT;
			post_client_list_size_recv(conn);
		}
	}

	return 0;
}

void reg_client_list_mem(connection *conn) {
	uint32_t num_clients = ntohl(clientList_size_buff);
	uint64_t cl_size = num_clients * sizeof(client);

	clientList_buff = (char *) realloc(clientList_buff, cl_size);
	if (clientList_mr)
		ibv_dereg_mr(clientList_mr);

	TEST_Z(clientList_mr = ibv_reg_mr(
				coord_ctx->pd,
				clientList_buff,
				cl_size,
				IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));

	printf("registered client list memory\n");
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
