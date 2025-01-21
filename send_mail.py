# Path: scripts/send_email_with_api.py
# Script para enviar un correo usando la API de SendGrid

import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

def send_email(log_file_path, subject, to_email, from_email):
    try:
        # Leer el contenido del archivo de log
        with open(log_file_path, 'r') as file:
            log_content = file.read()

        # Crear el correo con SendGrid API
        message = Mail(
            from_email=from_email,
            to_emails=to_email,
            subject=subject,
            html_content=f"<pre>{log_content}</pre>"
        )

        # Enviar el correo
        sg = SendGridAPIClient(os.getenv('SENDGRID_API_KEY'))
        response = sg.send(message)
        print(f"Correo enviado con éxito: {response.status_code}")
    except Exception as e:
        print(f"Error enviando correo: {e}")

if __name__ == "__main__":
    LOG_FILE = "output.log"
    SUBJECT = "Actualización de Base de Datos: Resumen de Ejecución Diaria"
    TO_EMAIL = "frentz233@gmail.com"
    FROM_EMAIL = "SE@CE Bot <trial-3yxj6lj8w1xgdo2r.mlsender.net>"  # Usa tu correo verificado en SendGrid

    send_email(LOG_FILE, SUBJECT, TO_EMAIL, FROM_EMAIL)
