from worker.types import UserPurchasesByStore


def merger_fn(merged: UserPurchasesByStore, payload: bytes) -> UserPurchasesByStore:
    message: UserPurchasesByStore = UserPurchasesByStore.deserialize(payload)

    if merged is None:
        return message

    for store_id, new_users in message.user_purchases_by_store.items():

        if not merged.user_purchases_by_store[store_id]:
            # si me llega una tienda que no tengo, ese es mi top 3 por ahora.
            merged.user_purchases_by_store[store_id] = new_users
        else:
            # si no, comparo el que tengo con los 3 nuevos para esa tienda
            current_top_3 = merged.user_purchases_by_store[store_id]
            combined = {**current_top_3, **new_users}
            final_top3 = dict(sorted(combined.items(), key=lambda x: x[1].purchases, reverse=True)[:3])
            merged.user_purchases_by_store[store_id] = final_top3

    return merged
