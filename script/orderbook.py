class LocalOrderBook:
    def __init__(self):
        self.bids = {}
        self.asks = {}

        self.update_id = None
        self.sequence = None
        self.synchronized = False

    def load_snapshot(self, snapshot):
        self.bids = {
            float(price): float(quantity)
            for price, quantity in snapshot["b"]
            if float(quantity) > 0
        }

        self.asks = {
            float(price): float(quantity)
            for price, quantity in snapshot["a"]
            if float(quantity) > 0
        }

        self.update_id = snapshot["u"]
        self.sequence = snapshot["seq"]
        self.synchronized = True

    def apply_delta(self, delta):
        if not self.synchronized:
            raise RuntimeError("Cannot apply a delta to an unsynchronized orderbook")
        self._update_side(self.bids, delta["b"])
        self._update_side(self.asks, delta["a"])

        self.update_id = delta["u"]
        self.sequence = delta["seq"]

    def invalidate(self):
        self.synchronized = False

    @staticmethod
    def _update_side(book_side, updates):
        for price, quantity in updates:
            price = float(price)
            quantity = float(quantity)

            if quantity == 0:
                book_side.pop(price, None)
            else:
                book_side[price] = quantity
