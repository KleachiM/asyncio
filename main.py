import asyncio
from asyncio import queues
import aiosqlite
from email.message import EmailMessage
import aiosmtplib


async def connect_db(db):
    conn = await aiosqlite.connect(db)
    result = await conn.execute("SELECT * FROM contacts")
    return await result.fetchall()


async def send_mail(first_name, last_name, email):
    message = EmailMessage()
    message["From"] = "login@host"
    message["To"] = email
    message["Subject"] = "Sent via aiosmtplib"
    message.set_content(f"Уважаемый {first_name} {last_name}! Спасибо, что пользуетесь нашим сервисом объявлений.")

    await aiosmtplib.send(message, hostname="hostname", port=25, username="username", password="password")


async def email_sender(Q):
    while True:
        email = Q.get()
        if email is None:
            await Q.put(None)
            break
        await send_mail(email['first_name'], email['last_name'], email['email'])


async def main():
    Q = queues.Queue(maxsize=5)
    senders = []
    for _ in range(10):
        sender = asyncio.create_task(email_sender(Q))
        senders.append(sender)
    db = await connect_db('contacts.db')
    for contact in db:
        contact = {'first_name': contact[1], 'last_name': contact[2], 'email': contact[3]}
        await Q.put(contact)
    await Q.put(None)
    for sender in senders:
        await sender

asyncio.run(main())