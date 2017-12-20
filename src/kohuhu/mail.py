from email.mime.text import MIMEText
import aiosmtplib


class GmailClient:
    """A GmailClient uses Gmail's SMTP servers to send emails"""

    def __init__(self, from_address, display_name, password):
        """
        Example usage:
        gmailClient = GmailClient("myaddress@gmail.com", "myname", "mypassword")
        await gmailClient.connect()
        await gmailClient.send_message("toSomebody@somedomain.com", "subject", "body")

        :param from_address: The Gmail address you are sending from
        :param display_name: The display name you want to show to recipients
        :param password: Your Gmail password for your from_address
        """
        self.from_address = from_address
        self.display_name = display_name
        self.password = password
        self.client = aiosmtplib.SMTP(hostname='smtp.gmail.com', port=587, use_tls=False)

    async def connect(self):
        """Connects to the SMTP server using the supplied credentials"""
        await self.client.connect()
        await self.client.starttls()
        await self.client.auth_login(self.from_address, self.password)

    async def send_message(self, to_addresses, subject, body):
        """
        Sends a message to the specified addresses. You must call connect() before sending a message.
        :param to_addresses: A semi-colon separated list of email addresses
        :param subject: The email subject
        :param body:
        """
        message = MIMEText(body)
        message['From'] = self.display_name
        message['To'] = to_addresses
        message['Subject'] = subject
        await self.client.send_message(message)


class KohuhuEmailer:
    """A KohuhuEmailer sends emails to recipients from the Kohuhu email account"""

    def __init__(self, config):
        """
        Example usage:

        import asyncio
        import json

        with open('../config.json') as config_file:
            config = json.load(config_file)
        emailer = KohuhuEmailer(config)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(emailer.send_email("Test", "TestBody"))

        :param config: The Kohuhu config dict
        """
        kohuhu_account = config['email']['kohuhu_account']
        self.client = GmailClient(kohuhu_account['username'], kohuhu_account['display_name'], kohuhu_account['password'])
        self.to_addresses = ';'.join(config['email']['recipients'])
        self.is_connected = False

    async def send_email(self, subject, body):
        """
        Sends a message to the specified addresses
        :param subject: The email subject
        :param body: The email body in plain text
        """
        if not self.is_connected:
            await self.client.connect()
            self.is_connected = True

        await self.client.send_message(self.to_addresses, subject, body)
