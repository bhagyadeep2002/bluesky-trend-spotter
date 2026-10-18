"""Deterministic, inspectable candidate grouping with no external NLP model."""

import re
import unicodedata
from dataclasses import dataclass
from urllib.parse import urlsplit

STOP = frozenset(
    """about after again against also always another anyone anything around
because been before being below between both could doing does down during each
even every everyone everything first from further getting going good great have
having here into just know like look make many more most much must need never
only other over people really said same says should some something still such
than that their them then there these they thing think this those through time
today together tomorrow very want well were what when where which while white
with without world would your yours yourself https http www com post posts
bluesky bsky social share shared follow following please thanks thank right
will you are the and for not but all can our out how was has had its his her
who why new now get too see one two don dont cant im ive its youre thats
three four five six seven eight nine ten eleven twelve thirteen fourteen fifteen
sixteen seventeen eighteen nineteen twenty thirty forty fifty sixty seventy
eighty ninety hundred hundreds thousand thousands million millions billion
billions trillion trillions zero digit digits number numbers word words
love back come comes came coming take takes took taken taking give gives given
getting got going went gone lets let away ever someone somebody anybody nobody
nothing things makes made making feel feels feeling thought thoughts sure maybe
actually literally pretty probably already almost enough little lots better best
last next once twice said say saying seen seeing tell told long much still
happy nice beautiful amazing awesome yes yeah okay true via use used using
each either neither own ours theirs themselves himself herself myself itself""".split()
)


@dataclass(frozen=True)
class Candidate:
    key: str
    label: str
    weight: float


def extract_candidates(text: str, limit: int = 12) -> list[Candidate]:
    normalized = unicodedata.normalize("NFC", text).casefold()
    candidates: dict[str, Candidate] = {}
    for hashtag in re.findall(r"(?<!\w)#([^\W_][\w]{1,63})", normalized):
        key = "hashtag:" + hashtag
        candidates[key] = Candidate(key, "#" + hashtag, 3)
    for url in re.findall(r"https?://[^\s<>]+", normalized):
        try:
            host = (urlsplit(url).hostname or "").removeprefix("www.").rstrip(".")
            if "." in host and len(host) <= 253 and host not in {"bsky.app", "t.co", "youtu.be"}:
                key = "domain:" + host
                candidates[key] = Candidate(key, host, 2)
        except ValueError:
            continue
    prose = re.sub(r"https?://\S+|[@#][\w.:-]+", " ", normalized)
    terms = set(re.findall(r"(?<!\w)[^\W\d_][\w]{3,39}(?!\w)", prose)) - STOP
    for term in terms:
        key = "term:" + term
        candidates[key] = Candidate(key, term, 1)
    return sorted(candidates.values(), key=lambda item: (-item.weight, item.key))[:limit]
