#!/usr/bin/make -f

.PHONY: test
test: deploy
	dfx ledger fabricate-cycles --amount 1000000000 --canister NacDBReorder_test
	dfx canister call NacDBReorder_test main '()'

.PHONY: deploy
deploy:
	dfx deploy