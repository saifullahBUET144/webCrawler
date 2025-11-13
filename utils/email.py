import logging
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from utils.config import settings

log = logging.getLogger(__name__)

async def send_alert_email(subject: str, body_html: str):
    """
    Sends an email alert using SendGrid.
    """
    # Check if the feature is configured
    if not settings.SENDGRID_API_KEY:
        log.warning("SENDGRID_API_KEY not set. Skipping email alert.")
        return
        
    if not settings.ALERT_SENDER_EMAIL or not settings.ALERT_RECIPIENT_EMAIL:
        log.warning("ALERT_SENDER_EMAIL or ALERT_RECIPIENT_EMAIL not set. Skipping email alert.")
        return

    log.info(f"Attempting to send email alert to {settings.ALERT_RECIPIENT_EMAIL}...")

    # Create the email message
    message = Mail(
        from_email=settings.ALERT_SENDER_EMAIL,
        to_emails=settings.ALERT_RECIPIENT_EMAIL,
        subject=subject,
        html_content=body_html
    )

    try:
        # Initialize the client and send
        sg = SendGridAPIClient(settings.SENDGRID_API_KEY)
        response = sg.send(message)
        
        log.info(f"Successfully sent email alert. Status code: {response.status_code}")
        
    except Exception as e:
        log.error(f"Failed to send email alert: {e}", exc_info=True)