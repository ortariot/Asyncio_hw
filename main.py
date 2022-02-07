import asyncio
import aiosmtplib
import aiosqlite

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

from more_itertools import chunked


QNTY_SEND_LIMIT = 50
SENDER = "sender@host.com"
HOSTNAME = "smtp.gmail.com"
PORT = "465"
USERNAME = "example"
PASSWORD = "password"


async def send_mail(first_name, last_name, recipient, addres):

    sender = SENDER
    msg = MIMEMultipart()
    msg.preamble = "happy birthday"
    msg["From"] = sender
    msg["To"] = recipient
    msg["Subject"] = "happy birthday"
    text = MIMEText(f"Hi {first_name} {last_name}!!! Haappy birthday"
                    f"my friend!! Your gift has been sent to {addres}",
                    "plain", "utf-8"
                    )
    msg.attach(text)

    await aiosmtplib.send(message=msg,
                          hostname=HOSTNAME,
                          port=PORT,
                          use_tls=True,
                          sender=sender,
                          username=USERNAME,
                          password=PASSWORD,
                          recipients=recipient
                          )


def mails_task(recipient_list):
    return (asyncio.create_task(send_mail(item[1], item[2],
                                          item[3], item[4]))
            for item in recipient_list)


async def mail_sender():
    recipient_list = await async_db_getter()
    tasks = mails_task(recipient_list)
    for tasks_part in chunked(tasks, QNTY_SEND_LIMIT):
        await asyncio.gather(*tasks_part)


async def async_db_getter():
    engine = create_async_engine('sqlite+aiosqlite:///contacts.db')
    async with engine.connect() as conn:
        result = await conn.execute(text("SELECT * FROM contacts"))
    return result.all()


if __name__ == "__main__":
    asyncio.run(mail_sender())
