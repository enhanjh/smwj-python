import bot.telegram_bot as tb




if __name__ == "__main__":
    chatbot = tb.TelegramBotSmwj()
    chatbot.start()
    chatbot.send_message("smwj-chatbot is starting up")