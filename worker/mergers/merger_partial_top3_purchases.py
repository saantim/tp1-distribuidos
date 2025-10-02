from shared.entity import StoreName, UserId
from worker.types import UserPurchasesByStore, UserPurchasesInfo


def merger_fn(merged: UserPurchasesByStore, payload: bytes) -> UserPurchasesByStore:
    message: UserPurchasesByStore = UserPurchasesByStore.deserialize(payload)

    if merged is None:
        return message

    for store_id, user_info in message.user_purchases_by_store.items():
        if store_id not in merged.user_purchases_by_store:
            merged.user_purchases_by_store[store_id] = {}

        for user_id, user_purchase_info in user_info.items():
            if user_id not in merged.user_purchases_by_store[store_id]:
                merged.user_purchases_by_store[store_id][user_id] = UserPurchasesInfo(
                    user=UserId(user_id), birthday="", purchases=0, store_name=StoreName("")
                )
            merged.user_purchases_by_store[store_id][user_id].purchases += user_purchase_info.purchases

    return merged
