import requests

def varios_destinatarios(mensaje):
    destinatarios = ['0998630405', '0994346133']
    try:
        for destinatario in destinatarios:
            enviar_mensaje(destinatario, mensaje)
        return {"success": True}
    except Exception as ex:
        print(f"Error al enviar el mensaje: {str(ex)}")
        return {"success": False, "error": str(ex)}

def enviar_mensaje(numero, mensaje):
    try:
        # Autenticación
        url_login = "https://serviciosexternos.farmaenlace.com:3002/usuarios/login"
        json_login = {"usuario": "externo", "password": "Externo.2019*"}
        headers_login = {"content-type": "application/json"}

        response_login = requests.post(
            url_login, json=json_login, headers=headers_login
        )
        datos = response_login.json()
        token = datos["token"]

        # Envío de mensajes
        url_whatsapp = (
            "https://serviciosexternos.farmaenlace.com:3002/GrupoMas/enviarWhastapp"
        )
        headers_whatsapp = {
            "content-type": "application/json",
            "Authorization": f"Bearer {token}",
        }

        json_whatsapp = {"celular": numero, "mensaje": mensaje, "sucursal": "003"}

        response_whatsapp = requests.post(
            url_whatsapp, json=json_whatsapp, headers=headers_whatsapp
        )
        datos = response_whatsapp.json()
        print(datos)

        return {"success": True}

    except Exception as ex:
        print(f"Error al enviar el mensaje: {str(ex)}")
        return {"success": False, "error": str(ex)}
