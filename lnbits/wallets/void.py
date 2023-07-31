from typing import AsyncGenerator

from loguru import logger

from ..core.models import Payment, PaymentStatus
from .base import InvoiceResponse, PaymentResponse, StatusResponse, Wallet


class VoidWallet(Wallet):
    async def create_invoice(self, *_, **__) -> InvoiceResponse:
        return InvoiceResponse(
            ok=False, error_message="VoidWallet cannot create invoices."
        )

    async def status(self) -> StatusResponse:
        logger.warning(
            (
                "This backend does nothing, it is here just as a placeholder, you must configure an "
                "actual backend before being able to do anything useful with LNbits."
            )
        )
        return StatusResponse(None, 0)

    async def pay_invoice(self, *_, **__) -> PaymentResponse:
        return PaymentResponse(
            ok=False, error_message="VoidWallet cannot pay invoices."
        )

    async def get_invoice_status(self, payment: Payment) -> PaymentStatus:
        return PaymentStatus(None)

    async def get_payment_status(self, payment: Payment) -> PaymentStatus:
        return PaymentStatus(None)

    async def paid_invoices_stream(self) -> AsyncGenerator[str, None]:
        yield ""
