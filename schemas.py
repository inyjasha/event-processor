from decimal import Decimal
from pydantic import AwareDatetime, BaseModel, Field, StringConstraints
from typing import Annotated


ClickId = Annotated[
    str,
    StringConstraints(min_length=1, pattern=r"\S")
]

Money = Annotated[
    Decimal,
    Field(allow_inf_nan=False)
]

AdId = Annotated[
    int,
    Field(
        strict=True,
        ge=-(2**63),
        le=2**63-1
    ),
]

class ClickEvent(BaseModel):
    clid: ClickId
    ad_id: AdId
    click_spend: Money
    ts: AwareDatetime


class PaymentEvent(BaseModel):
    clid: ClickId
    payout: Money
    ts: AwareDatetime


