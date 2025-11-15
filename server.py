#2023EBCS402
import time
import threading
import uuid
from concurrent import futures

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

import banking_pb2 as pb
import banking_pb2_grpc as pb_grpc


class InMemoryBank:
    def __init__(self):
        self._accounts = {}
        self._transactions = {}  # user_id -> [transactions]
        self._lock = threading.RLock()

    def create_account(self, user_id: str, initial_balance: float = 0.0):
        with self._lock:
            if user_id in self._accounts:
                return False
            self._accounts[user_id] = float(initial_balance)
            self._transactions.setdefault(user_id, [])
            return True

    def get_balance(self, user_id: str):
        with self._lock:
            if user_id not in self._accounts:
                raise KeyError("user not found")
            return float(self._accounts[user_id])

    def update_balance(self, user_id: str, delta: float):
        with self._lock:
            if user_id not in self._accounts:
                raise KeyError("user not found")
            new_bal = float(self._accounts[user_id]) + float(delta)
            # optionally disallow negative balances here
            self._accounts[user_id] = new_bal
            return new_bal

    def add_transaction(self, tx: dict):
        with self._lock:
            # add tx for from_user and to_user where applicable
            from_u = tx.get("from_user_id")
            to_u = tx.get("to_user_id")
            if from_u:
                self._transactions.setdefault(from_u, []).append(tx)
            if to_u and to_u != from_u:
                self._transactions.setdefault(to_u, []).append(tx)

    def get_transactions(self, user_id: str, limit: int = 0):
        with self._lock:
            if user_id not in self._transactions:
                raise KeyError("user not found")
            txs = list(self._transactions.get(user_id, []))
            txs.sort(key=lambda x: x["created_at"], reverse=True)
            if limit and limit > 0:
                return txs[:limit]
            return txs


bank = InMemoryBank()


def make_timestamp(ts_seconds: float):
    t = Timestamp()
    t.FromSeconds(int(ts_seconds))
    return t


class AccountService(pb_grpc.AccountServiceServicer):
    def GetBalance(self, request, context):
        user_id = request.user_id
        try:
            bal = bank.get_balance(user_id)
            return pb.GetBalanceResponse(user_id=user_id, balance=bal, message="OK")
        except KeyError:
            context.abort(grpc.StatusCode.NOT_FOUND, f"User {user_id} not found")

    def UpdateBalance(self, request, context):
        user_id = request.user_id
        delta = request.delta
        tx_id = request.transaction_id or str(uuid.uuid4())
        try:
            new_bal = bank.update_balance(user_id, delta)
            return pb.UpdateBalanceResponse(user_id=user_id, new_balance=new_bal, message=f"Updated by {delta}, tx={tx_id}")
        except KeyError:
            context.abort(grpc.StatusCode.NOT_FOUND, f"User {user_id} not found")


class TransactionService(pb_grpc.TransactionServiceServicer):
    def InitiateTransfer(self, request, context):
        from_user = request.from_user_id
        to_user = request.to_user_id
        amount = request.amount

        if amount <= 0:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Transfer amount must be > 0")

        # check both accounts exist
        try:
            _ = bank.get_balance(from_user)
            _ = bank.get_balance(to_user)
        except KeyError as e:
            context.abort(grpc.StatusCode.NOT_FOUND, str(e))

        tx_id = str(uuid.uuid4())
        ts = time.time()

        # atomic transfer: check funds then debit/credit
        with bank._lock:
            from_balance = bank.get_balance(from_user)
            if from_balance < amount:
                # record failed transaction
                tx = {
                    "transaction_id": tx_id,
                    "from_user_id": from_user,
                    "to_user_id": to_user,
                    "amount": amount,
                    "type": pb.TransactionType.TRANSFER,
                    "status": pb.TransactionStatus.FAILED,
                    "created_at": ts,
                    "note": "insufficient funds",
                }
                bank.add_transaction(tx)
                context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Insufficient funds")

            # apply updates
            new_from = bank.update_balance(from_user, -amount)
            new_to = bank.update_balance(to_user, amount)

            tx = {
                "transaction_id": tx_id,
                "from_user_id": from_user,
                "to_user_id": to_user,
                "amount": amount,
                "type": pb.TransactionType.TRANSFER,
                "status": pb.TransactionStatus.COMPLETED,
                "created_at": ts,
                "note": f"from {from_user} to {to_user}",
            }
            bank.add_transaction(tx)

        return pb.InitiateTransferResponse(success=True, transaction_id=tx_id, message="Transfer completed")

    def GetTransactionHistory(self, request, context):
        user_id = request.user_id
        limit = request.limit
        try:
            txs = bank.get_transactions(user_id, limit)
        except KeyError:
            context.abort(grpc.StatusCode.NOT_FOUND, f"User {user_id} not found")

        proto_txs = []
        for t in txs:
            ts = make_timestamp(t["created_at"])
            proto_txs.append(
                pb.Transaction(
                    transaction_id=t["transaction_id"],
                    from_user_id=t.get("from_user_id", ""),
                    to_user_id=t.get("to_user_id", ""),
                    amount=t.get("amount", 0.0),
                    type=t.get("type", pb.TransactionType.UNKNOWN),
                    status=t.get("status", pb.TransactionStatus.COMPLETED),
                    created_at=ts,
                    note=t.get("note", ""),
                )
            )
        return pb.GetTransactionHistoryResponse(transactions=proto_txs)


def serve(host="0.0.0.0", port=50051):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb_grpc.add_AccountServiceServicer_to_server(AccountService(), server)
    pb_grpc.add_TransactionServiceServicer_to_server(TransactionService(), server)
    server.add_insecure_port(f"{host}:{port}")
    server.start()
    print(f"gRPC server running on {host}:{port}")

    # create some demo accounts
    bank.create_account("alice", 1000.0)
    bank.create_account("bob", 500.0)
    bank.create_account("charlie", 0.0)

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    serve()
