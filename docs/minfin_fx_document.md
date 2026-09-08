# Minfin FX announcement document capture

`python -m moex_research.external_data.minfin_fx_document --post-id 9132 --output PATH`
reads one explicitly chosen public Telegram post, plus two independently published
government references identifying the channel. It sends no messages and requires no
Telegram account. Standard HTTPS verification remains enabled; redirects, oversized
responses and routes outside the fixed source scope are refused.

The Ministry of Transport directly identifies https://t.me/minfin/6717 as its source at
https://mintrans.gov.ru/press-center/branch-news/6845 and https://t.me/minfin/7910 at
https://mintrans.gov.ru/press-center/branch-news/8257. Both exact anchors are checked and
their raw HTML and receipt times archived for every capture. These are independent
government references to the channel; they do not establish that any specific post is
the latest announcement or that the Ministry of Finance website is currently reachable.

The document parser requires the exact post identity, one aware publication timestamp,
one official announcement link, consistent operation direction, date interval, and
positive total/daily ruble equivalents. Numeric strings preserve decimal precision.
The supported wording describes an announced plan to buy/sell foreign currency AND
gold; it does not establish execution or a currency-specific amount. Other wording,
including suspension announcements and different sale formulations, fails closed.

The Telegram timestamp is explicitly separate from the unknown official-site release
timestamp. Raw HTML and a JSON manifest are stored by SHA-256 with exclusive writes;
the response body can change as neighboring posts/views change. This is a receipt of
the exact response, not a stable revision id for the selected post alone.

No automatic latest selection, production API admission, historical PIT acceptance,
model use, forecast direction, consensus surprise or trading authority is granted.
These require separate contracts and acceptance. No full-macro blocker is cleared.
