import bot.telegram_bot as tb  # python-telegram-bot
import msg.kafkaMsg as km
import util.logUtil as lu


if __name__ == "__main__":
    # 1. logger start
    loggerMain = lu.LoggerSmwj()
    # loggerMain.propagate = 0
    
    # 2. telegram bot start
    chatbot = tb.TelegramBotSmwj()
    chatbot.start()
