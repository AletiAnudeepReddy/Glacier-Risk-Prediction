from twilio.rest import Client

client = Client("AC787fcee289e3ba8a6f2319e51ac760d0", "2c59a7305a85a4ed816578be41b4d0b1")

verification = client.verify.services("VA5b727b9cc667d2fdd87efbd49f209944").verifications.create(
    to="+919014384195",
    channel="sms"
)

print("Status:", verification.status)