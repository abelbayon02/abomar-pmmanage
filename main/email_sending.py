import yagmail

def send_email(body, subject):
    # Your Gmail address
    sender_email = 'abomar.dtf.api@gmail.com'
    # The app password
    app_password = 'xcjw tphv uzjt ctwh'

    # Initialize yagmail client
    yag = yagmail.SMTP(sender_email, app_password)

    # Set up the email content
    recipients = [
        'RawalPankaj@johndeere.com',
        'ramajam@abomar.net',
        'abbanay@lmiitsolutions.com',
        'marco@abomar.net'
    ]
    cc=['punzalanpatrickjason@gmail.com']
    bcc=['ibayonabel@gmail.com']

    # Set the custom "From" header with your desired display name
    from_name = 'Abomar Notification'
    yag.send(
        to=recipients,
        cc=cc,
        bcc=bcc,
        subject=subject,
        contents=body,
        headers={'From': f'{from_name} <{sender_email}>'}
    )

    print('Email sent successfully!')
