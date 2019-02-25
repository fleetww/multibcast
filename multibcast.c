#include "multibcast.h"
#include <stdio.h>

int main() {
	printf("Hello World\n");
	return 0;
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
