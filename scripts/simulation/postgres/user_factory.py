"""User Factory — Phase 2: PostgreSQL Entity Generation (Users)

Generates user attributes for PostgreSQL simulation.
See README.md#phase-markers-used-in-code-comments
"""

from faker import Faker

from ..id_generator import IDGenerator
from ..random_provider import RandomProvider


class UserFactory:

    COUNTRIES = (
        "KSA",
        "UAE",
        "EGY",
        "KWT",
        "QAT",
        "MAR",
        "JOR",
    )

    def __init__(
        self,
        *,
        random_provider: RandomProvider,
    ):
        self.random = random_provider
        self.faker = Faker()

    def generate_user_id(self) -> str:
        return IDGenerator.generate()

    def generate_email(self) -> str:
        return self.faker.email()

    def generate_country(self) -> str:
        return self.random.choice(
            self.COUNTRIES
        )