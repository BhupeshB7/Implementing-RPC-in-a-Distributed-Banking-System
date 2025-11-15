#2023EBCS402
import grpc
import time

import banking_pb2 as pb
import banking_pb2_grpc as pb_grpc


def get_balance(stub, user_id):
    req = pb.GetBalanceRequest(user_id=user_id)
    resp = stub.GetBalance(req)
    return resp

 
def initiate_transfer(stub, from_user, to_user, amount):
    req = pb.InitiateTransferRequest(from_user_id=from_user, to_user_id=to_user, amount=amount)
    resp = stub.InitiateTransfer(req)
    return resp

def get_history(stub, user_id, limit=0):
    req = pb.GetTransactionHistoryRequest(user_id=user_id, limit=limit)
    resp = stub.GetTransactionHistory(req)
    return resp


def demo():
    channel = grpc.insecure_channel("localhost:50051")
    account_stub = pb_grpc.AccountServiceStub(channel)
    tx_stub = pb_grpc.TransactionServiceStub(channel)

    print("Initial balances:")
    for user in ("alice", "bob", "charlie"):
        try:
            r = account_stub.GetBalance(pb.GetBalanceRequest(user_id=user))
            print(f"  {user}: {r.balance}")
        except grpc.RpcError as e:
            print(f"  getBalance {user} error: {e.details()}")

    print("\nTransfer 200 from alice to bob")
    try:
        res = tx_stub.InitiateTransfer(pb.InitiateTransferRequest(from_user_id="alice", to_user_id="bob", amount=200.0))
        print("  Transfer success:", res.success, "tx:", res.transaction_id)
    except grpc.RpcError as e:
        print("  Transfer failed:", e.code(), e.details())

    print("\nBalances after transfer:")
    for user in ("alice", "bob"):
        r = account_stub.GetBalance(pb.GetBalanceRequest(user_id=user))
        print(f"  {user}: {r.balance}")

    print("\nAttempt transfer with insufficient funds: charlie -> alice amount 50")
    try:
        res = tx_stub.InitiateTransfer(pb.InitiateTransferRequest(from_user_id="charlie", to_user_id="alice", amount=50.0))
        print("  Transfer success:", res.success)
    except grpc.RpcError as e:
        print("  Transfer failed:", e.code(), e.details())

    print("\nTransaction history for alice (most recent first):")
    hist = tx_stub.GetTransactionHistory(pb.GetTransactionHistoryRequest(user_id="alice", limit=10))
    for tx in hist.transactions:
        ttime = tx.created_at.seconds
        print(f"  tx={tx.transaction_id} from={tx.from_user_id} to={tx.to_user_id} amount={tx.amount} status={tx.status} time={time.ctime(ttime)} note={tx.note}")

    channel.close()


if __name__ == "__main__":
    demo()
