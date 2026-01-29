import smtplib
from email.mime.text import MIMEText
from email.message import EmailMessage

EMAIL = "gunetanmay@gmail.com"
PASSWORD = "mzei xogc zwqw gsyu"
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 465

def send_email(to_mail: str, anomaly: dict):
    subject = "Anomaly Detected in Log Data"
    body = f"""
    An anomaly has been detected in system logs

    timewindow: {anomaly['timestamp']}
    Error Count: {anomaly['error_count']}
    z score: {anomaly['z_score']}

    please review log data

    Regards,
    Tanmay
    """

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = EMAIL
    msg["To"] = to_email
    msg.set_content(body)

    with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
        server.login(EMAIL, PASSWORD)
        server.send_message(msg)