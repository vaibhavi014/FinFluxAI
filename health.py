from flask import Blueprint, current_app, jsonify
from sqlalchemy import text

from finflux.extensions import db

health_bp = Blueprint("health", __name__)


@health_bp.get("/actuator/health")
def actuator_health():
    db_status = "UP"
    try:
        db.session.execute(text("SELECT 1"))
    except Exception:
        db_status = "DOWN"
    kafka_status = "UP"
    try:
        from kafka.admin import KafkaAdminClient

        settings = current_app.config["SETTINGS"]
        admin = KafkaAdminClient(
            bootstrap_servers=settings.kafka_bootstrap.split(","),
            client_id="finflux-health",
            request_timeout_ms=2000,
        )
        admin.list_topics()
        admin.close()
    except Exception:
        kafka_status = "DOWN"
    overall = "UP" if db_status == "UP" and kafka_status == "UP" else "DOWN"
    code = 200 if overall == "UP" else 503
    return jsonify(
        {
            "status": overall,
            "components": {
                "db": {"status": db_status},
                "kafka": {"status": kafka_status},
            },
        }
    ), code
