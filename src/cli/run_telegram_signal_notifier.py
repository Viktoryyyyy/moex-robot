from dotenv import load_dotenv
from src.bot.telegram_signal_notifier import run


def main():
    load_dotenv()
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
