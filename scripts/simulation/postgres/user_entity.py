"""User Entity — Phase 2: PostgreSQL Entity Generation (User Schema)

Schema definition for a PostgreSQL user entity.
See README.md#phase-markers-used-in-code-comments
"""

from datetime import datetime

class UserEntity:

    def __init__(
        self,
        *,
        user_id: str,
        email: str | None,
        country: str,
        account_status: str,
        created_at: datetime,
        last_login: datetime | None = None,
    ):
        self.user_id = user_id
        self.email = email
        self.country = country
        self.account_status = account_status
        self.created_at = created_at
        self.last_login = last_login

    def to_dict(self) -> dict:
        return {
            "user_id": self.user_id,
            "email": self.email,
            "country": self.country,
            "account_status": self.account_status,
            "created_at": self.created_at,
            "last_login": self.last_login,
        }