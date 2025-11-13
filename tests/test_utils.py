import pytest
from utils.config import settings
from utils.email import send_alert_email

@pytest.mark.asyncio
async def test_send_alert_email_success(mocker, monkeypatch):
    """
    Test that send_alert_email tries to send a correctly formatted email.
    """
    mock_sg_client_class = mocker.patch('utils.email.SendGridAPIClient')
    
    # Create a mock instance and a mock send method
    mock_sg_instance = mocker.MagicMock()
    mock_sg_client_class.return_value = mock_sg_instance
    mock_sg_instance.send.return_value = mocker.MagicMock(status_code=202)

    monkeypatch.setattr(settings, "SENDGRID_API_KEY", "test-api-key")
    monkeypatch.setattr(settings, "ALERT_SENDER_EMAIL", "sender@example.com")
    monkeypatch.setattr(settings, "ALERT_RECIPIENT_EMAIL", "recipient@example.com")

    test_subject = "Test Subject"
    test_body = "<h1>Test Body</h1>"
    await send_alert_email(test_subject, test_body)

    # initialized with correct API key
    mock_sg_client_class.assert_called_with("test-api-key")
    
    # Csend was called once
    mock_sg_instance.send.assert_called_once()
    
    # Get the 'message' object 
    sent_message = mock_sg_instance.send.call_args[0][0]

    assert sent_message.from_email.email == "sender@example.com"    
    assert sent_message.personalizations[0].tos[0]['email'] == "recipient@example.com"    
    assert sent_message.subject.subject == test_subject
    assert sent_message.contents[0].content == test_body
    

@pytest.mark.asyncio
async def test_send_alert_email_skips_if_no_key(mocker, monkeypatch, caplog):
    """
    Test that no email is sent if the API key is not configured.
    """
    mock_sg_client_class = mocker.patch('utils.email.SendGridAPIClient')

    # Set the API key to be empty
    monkeypatch.setattr(settings, "SENDGRID_API_KEY", "")

    await send_alert_email("Test", "Test")

    # client not called
    mock_sg_client_class.assert_not_called()
    
    assert "SENDGRID_API_KEY not set" in caplog.text