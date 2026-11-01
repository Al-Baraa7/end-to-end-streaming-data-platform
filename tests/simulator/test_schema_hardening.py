"""
Test Schema Hardening - R15
"""

import pytest
from datetime import datetime, timezone

from scripts.simulation.postgres.payment_entity import PaymentEntity
from scripts.simulation.postgres.payment_factory import PaymentFactory
from scripts.simulation.random_provider import RandomProvider


class TestSchemaHardening:
    """Verify R15 schema hardening: attempt_no, billing_reason, FK constraints."""

    def test_payment_entity_has_attempt_no(self):
        """PaymentEntity has attempt_no field with valid range 1-3."""
        payment = PaymentEntity(
            payment_id="pay_1", subscription_id="sub_1", user_id="user_1",
            amount=9.99, currency="USD", payment_date=datetime(2026,1,1,tzinfo=timezone.utc),
            payment_status="success", attempt_no=2, billing_reason="retry"
        )
        assert payment.attempt_no == 2
        assert 1 <= payment.attempt_no <= 3

    def test_payment_entity_has_billing_reason(self):
        """PaymentEntity has billing_reason field with valid enum values."""
        for reason in ("initial", "retry", "renewal"):
            payment = PaymentEntity(
                payment_id="pay_1", subscription_id="sub_1", user_id="user_1",
                amount=9.99, currency="USD", payment_date=datetime(2026,1,1,tzinfo=timezone.utc),
                payment_status="success", attempt_no=1, billing_reason=reason
            )
            assert payment.billing_reason == reason

    def test_payment_entity_user_id_field(self):
        """PaymentEntity has user_id field for FK constraint."""
        payment = PaymentEntity(
            payment_id="pay_1", subscription_id="sub_1", user_id="user_1",
            amount=9.99, currency="USD", payment_date=datetime(2026,1,1,tzinfo=timezone.utc),
            payment_status="success"
        )
        assert hasattr(payment, "user_id")
        assert payment.user_id == "user_1"

    def test_payment_to_dict_includes_new_fields(self):
        """PaymentEntity.to_dict() includes attempt_no, billing_reason, user_id."""
        payment = PaymentEntity(
            payment_id="pay_1", subscription_id="sub_1", user_id="user_1",
            amount=9.99, currency="USD", payment_date=datetime(2026,1,1,tzinfo=timezone.utc),
            payment_status="success", attempt_no=2, billing_reason="retry"
        )
        d = payment.to_dict()
        assert "attempt_no" in d
        assert "billing_reason" in d
        assert "user_id" in d
        assert d["attempt_no"] == 2
        assert d["billing_reason"] == "retry"
        assert d["user_id"] == "user_1"

    def test_payment_factory_validates_attempt_no(self):
        """PaymentFactory validates attempt_no range 1-3."""
        random_provider = RandomProvider(seed=42)
        factory = PaymentFactory(random_provider=random_provider)
        
        # Valid attempt_no
        payment = factory.create_attempt(
            subscription_id="sub_1", user_id="user_1", plan_type="premium",
            subscription_duration_days=30, payment_date=datetime(2026,1,1,tzinfo=timezone.utc),
            attempt_no=2, billing_reason="retry"
        )
        assert payment.attempt_no == 2
        
        # Invalid attempt_no should raise
        with pytest.raises(ValueError, match="attempt_no must be in"):
            factory.create_attempt(
                subscription_id="sub_1", user_id="user_1", plan_type="premium",
                subscription_duration_days=30, payment_date=datetime(2026,1,1,tzinfo=timezone.utc),
                attempt_no=4, billing_reason="retry"
            )

    def test_payment_factory_validates_billing_reason(self):
        """PaymentFactory validates billing_reason enum."""
        random_provider = RandomProvider(seed=42)
        factory = PaymentFactory(random_provider=random_provider)
        
        # Valid billing_reason
        for reason in ("initial", "retry", "renewal"):
            payment = factory.create_attempt(
                subscription_id="sub_1", user_id="user_1", plan_type="premium",
                subscription_duration_days=30, payment_date=datetime(2026,1,1,tzinfo=timezone.utc),
                attempt_no=1, billing_reason=reason
            )
            assert payment.billing_reason == reason
        
        # Invalid billing_reason should raise
        with pytest.raises(ValueError, match="billing_reason must be one of"):
            factory.create_attempt(
                subscription_id="sub_1", user_id="user_1", plan_type="premium",
                subscription_duration_days=30, payment_date=datetime(2026,1,1,tzinfo=timezone.utc),
                attempt_no=1, billing_reason="invalid"
            )