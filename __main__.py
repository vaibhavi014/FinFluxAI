import os

from finflux import create_app

if __name__ == "__main__":
    application = create_app()
    port = int(os.environ.get("APP_PORT", "8080"))
    application.run(host="0.0.0.0", port=port, debug=False)
