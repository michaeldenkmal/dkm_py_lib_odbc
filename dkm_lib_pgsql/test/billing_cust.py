from dataclasses import dataclass
import datetime as dt

@dataclass(slots=True)
class BillingCustRow:
    id: int
    created_at: dt.datetime
    updated_at: dt.datetime
    name: str
    email: str
    vat_id: str
    address: str
    offh_gp_id: Optional[float]