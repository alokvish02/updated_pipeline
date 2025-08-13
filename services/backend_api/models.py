from extensions import db  # Use the shared db instance
from datetime import datetime

from sqlalchemy import false


class TradeModel(db.Model):
    __tablename__ = 'trade'
    __table_args__ = {'schema': 'public'}

    id = db.Column(db.String, primary_key=True)
    symbol = db.Column(db.String, nullable=False)
    candle_time = db.Column(db.DateTime, nullable=False)
    action = db.Column(db.String, nullable=False)
    price = db.Column(db.Float, nullable=False)
    stop_loss = db.Column(db.Float, nullable=True)
    target_price = db.Column(db.Float, nullable=True)
    status = db.Column(db.String, nullable=False)
    executed_at = db.Column(db.DateTime, nullable=True)
    current_price = db.Column(db.Float, nullable=True)
    pnl = db.Column(db.Float, nullable=True)

    def to_dict(self):
        return {
            "id": self.id,
            "symbol": self.symbol,
            "candle_time": self.candle_time.isoformat() if self.candle_time else None,  # Convert datetime
            "action": self.action,
            "price": self.price,
            "stop_loss": self.stop_loss,
            "target_price": self.target_price,
            "status": self.status,
            "executed_at": self.executed_at.isoformat() if self.executed_at else None,  # Convert datetime
            "current_price": self.current_price,
            "pnl": self.pnl
        }


class TradeHistoryModel(db.Model):
    __tablename__ = 'trade_history'

    id = db.Column(db.String, primary_key=True)
    symbol = db.Column(db.String, nullable=False)
    candle_time = db.Column(db.DateTime, nullable=False)
    action = db.Column(db.String, nullable=False)
    entry_price = db.Column(db.Float, nullable=False)
    exit_price = db.Column(db.Float, nullable=False)
    pnl = db.Column(db.Float, nullable=False)
    status = db.Column(db.String, nullable=False)
    executed_at = db.Column(db.DateTime, nullable=True)
    stop_loss = db.Column(db.Float, nullable=False)
    target_price = db.Column(db.Float, nullable=False)

    def to_dict(self):
        return {
            "id": self.id,
            "symbol": self.symbol,
            "candle_time": self.candle_time.isoformat() if self.candle_time else None,  # Convert datetime
            "action": self.action,
            "entry_price": self.entry_price,
            "exit_price": self.exit_price,
            "pnl": self.pnl,
            "status": self.status,
            "executed_at": self.executed_at.isoformat() if self.executed_at else None,
            "stop_loss": self.stop_loss,
            "target_price": self.target_price,
        }

#old mukesh notification code
# class NotificationModel(db.Model):
#     __tablename__ = 'notification'
#
#     action = db.Column(db.String(10), primary_key=True)  # Action (BUY, SELL, etc.)
#     symbol = db.Column(db.String(50), primary_key=True)   # Stock ticker symbol
#     created_at = db.Column(
#         db.DateTime, primary_key=True, server_default=db.func.current_timestamp()
#     )  # Record creation time
#     price = db.Column(db.Numeric(12, 2), nullable=False)          # Price value
#     stop_loss = db.Column(db.Numeric(12, 2), nullable=False)        # Stop loss value
#     target_price = db.Column(db.Numeric(12, 2), nullable=False)     # Target price value
#     execution_time = db.Column(db.DateTime, nullable=False)         # Time of execution
#     message = db.Column(db.Text, nullable=False)
#     seen = db.Column(db.Boolean, default=False)
#     exchange_mode = db.Column(db.String(10))
#
#     def to_dict(self):
#         return {
#             "action": self.action,
#             "symbol": self.symbol,
#             "price": round(float(self.price),2) if self.price is not None else None,
#             "stop_loss": round(float(self.stop_loss),2) if self.stop_loss is not None else None,
#             "target_price": round(float(self.target_price),2) if self.target_price is not None else None,
#             "execution_time": self.execution_time.isoformat() if self.execution_time else None,
#             "message": self.message,
#             "created_at": self.created_at.isoformat() if self.created_at else None,
#             "seen": self.seen,
#             "exchange_mode": self.exchange_mode
#         }

class NotificationModel(db.Model):
    __tablename__ = 'notification'

    id = db.Column(db.Integer, primary_key=True)  # Match the database PK
    action = db.Column(db.String(10))
    symbol = db.Column(db.String(50))
    created_at = db.Column(db.DateTime(timezone=True), server_default=db.func.current_timestamp())
    price = db.Column(db.Numeric(12, 2), nullable=True)  # Allow NULL if needed
    stop_loss = db.Column(db.Numeric(12, 2), nullable=True)
    target_price = db.Column(db.Numeric(12, 2), nullable=True)
    execution_time = db.Column(db.DateTime, nullable=True)  # Allow NULL
    message = db.Column(db.Text)
    seen = db.Column(db.Boolean, default=False)
    exit_price = db.Column(db.Numeric(12, 2), nullable=True)
    pnl = db.Column(db.Numeric(12, 2), nullable=True)
    status = db.Column(db.String(50))
    exchange_mode = db.Column(db.String(10))

    def to_dict(self):
        return {
            "id": self.id,
            "action": self.action,
            "symbol": self.symbol,
            "price": round(float(self.price), 2) if self.price is not None else None,
            "stop_loss": round(float(self.stop_loss), 2) if self.stop_loss is not None else None,
            "target_price": round(float(self.target_price), 2) if self.target_price is not None else None,
            "execution_time": self.execution_time.isoformat() if self.execution_time else None,
            "message": self.message,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "seen": self.seen,
            "exit_price": round(float(self.exit_price), 2) if self.exit_price is not None else None,
            "pnl": round(float(self.pnl), 2) if self.pnl is not None else None,
            "status": self.status,
            "exchange_mode": self.exchange_mode
        }

class SpreadsModel(db.Model):
    __tablename__ = 'spreads'

    symbol = db.Column(db.String(10), primary_key=True)  # Stock ticker symbol


    def to_dict(self):
        return {
            "symbol": self.symbol
        }
