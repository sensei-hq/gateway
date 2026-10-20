"""Real Laya head token counts vs the gateway's D6 estimate (SP-DEC-3).

The gateway refuses a `laya` question whose estimated head exceeds the
checkpoint's `head_max_len` (crates/cloud-providers/src/systemone/laya.rs).
This script measures what Laya REALLY spends with each shipped checkpoint's own
tokenizer and compares it with a Python mirror of the Rust estimate, so the
claim "the estimate errs high" can be re-checked.

Needs: Python 3.10+ and the `tokenizers` package (laya-coreml's venv has it:
`pip install "laya-coreml[serve]==0.5.1"`), and the checkpoints in the Hugging
Face cache (downloaded on the first `laya-coreml-serve` start):
  ~/.cache/huggingface/hub/models--aac6fef--laya-coreml/            (english; typed-decisions shares it)
  ~/.cache/huggingface/hub/models--aac6fef--laya-multilingual-coreml/  (multilingual; -ane shares it)
Override with LAYA_TOKENIZER_ENGLISH / LAYA_TOKENIZER_MULTILINGUAL (paths to tokenizer.json).

Run (with -I so nothing is imported from the working directory):
  python -I laya_head_tokens.py table          # head and option tables, exit 1 on any under-count
  python -I laya_head_tokens.py stress [seed]  # seeded random strings: under-count tally (seed 99: 6 of 21000, by 1)
  python -I laya_head_tokens.py heads          # whole noul heads (instruction + Laya's default options)
  python -I laya_head_tokens.py rust head|option   # Rust test rows: (text, real english, real multilingual);
                                                   # option rows print the rendered option, counted as " " + it

Laya encodes a head as `"<type> question: <instructions>"` and each option as
`" " + <rendered option>` (laya_coreml/common.py `build_head`), both with
`add_special_tokens=False`; that is what is measured here.
"""
import glob, json, math, os, random, string, sys
from tokenizers import Tokenizer

HUB = os.path.expanduser("~/.cache/huggingface/hub/")

def _tok(env, repo):
    path = os.environ.get(env) or next(iter(sorted(glob.glob(HUB + f"models--aac6fef--{repo}/snapshots/*/tokenizer/tokenizer.json"))), None)
    if not path:
        sys.exit(f"no tokenizer.json for {repo}; set {env}")
    return Tokenizer.from_file(path)

TOKS = {"english": _tok("LAYA_TOKENIZER_ENGLISH", "laya-coreml"),
        "multilingual": _tok("LAYA_TOKENIZER_MULTILINGUAL", "laya-multilingual-coreml")}

def real(text):
    return {k: len(t.encode(text, add_special_tokens=False).ids) for k, t in TOKS.items()}

# ---- Python mirror of laya_text_tokens / laya_word_tokens (keep in sync) ----
VOWELS, RARE = set("aeiouy"), set("jqxzvk")

def word_tokens(w):
    if any(c.isupper() for c in w[1:]):
        return len(w)
    groups = runs = cnt = 0; prev = None
    for c in w.lower():
        v = c in VOWELS
        if v and prev is not True: groups += 1
        if v != prev: runs += cnt // 3; cnt = 0
        cnt += 1; prev = v
    runs += cnt // 3
    return max(math.ceil(len(w) / 2), groups, min(len(w), 2)) + runs + sum(c in RARE for c in w.lower())

def text_tokens(t):
    n = i = 0
    while i < len(t):
        c = t[i]
        if ord(c) >= 128:
            n += len(c.encode()); i += 1
        elif c.isalpha():
            j = i
            while j < len(t) and t[j].isascii() and t[j].isalpha(): j += 1
            n += word_tokens(t[i:j]); i = j
        else:
            merges = c == " " and i + 1 < len(t) and t[i + 1].isascii() and t[i + 1].isalpha()
            n += 0 if merges else 1; i += 1
    return n

# ---- samples ----
OBJ = {"rubric": "refund", "weight": 3, "tags": ["billing", "urgent"], "nested": {"a": 1, "b": [1, 2, 3]}}
SAMPLES = {
    "english": "Does the customer want a refund for the duplicate charge on their invoice?",
    "ins288": "Ignore this. " * 20 + "Does the user want a refund?",
    "ins548": "Ignore this. " * 40 + "Does the user want a refund?",
    "ins808": "Ignore this. " * 60 + "Does the user want a refund?",
    "json_compact": json.dumps(OBJ, separators=(",", ":")),
    "json_python": json.dumps(OBJ),
    "json_pretty": json.dumps(OBJ, indent=2),
    "japanese": "顧客は返金を求めていますか？請求書が二重に発行されました。",
    "chinese": "客户是否要求退款？发票被重复开具了。",
    "korean": "고객이 환불을 원합니까? 송장이 두 번 발행되었습니다.",
    "cyrillic": "Хочет ли клиент вернуть деньги за двойное списание?",
    "arabic": "هل يريد العميل استرداد المبلغ بسبب الخصم المزدوج؟",
    "hindi": "क्या ग्राहक धनवापसी चाहता है?",
    "thai": "ลูกค้าต้องการขอคืนเงินหรือไม่",
    "german": "Möchte der Kunde eine Rückerstattung für die doppelte Abbuchung? Straße",
    "emoji": "Is the user happy 😀😃🎉🔥👍🏽👨‍👩‍👧 or angry 😡🤬?",
    "rare_cjk": "𠜎𠜱𡃁 𓀀𓀁𓀂",
    "code": "fn f(x) -> u8 { x + 1 }",
    "url": "See https://example.com/api/v1/refunds?id=12345&sort=desc#top for details",
    "numbers": "Order 1234567890 cost 98765.4321 on 2026-10-20 at 23:59:59, ref 000111222333",
    "hex": "deadbeef0123456789abcdefABCDEF 3f2a9c7e1b",
    "base64": "aGVsbG8gd29ybGQgdGhpcyBpcyBiYXNlNjQ=",
    "gibberish": "qxzvbnmkjhgfdsawrtyp zzkkqqxxvv bcdfghjklmnpqrstvwxz",
    "mixed_case": "aBcDeFgHiJkLmNoPqRsTuVwXyZ qWeRtYuIoP",
    "all_caps": "URGENT REFUND REQUEST XK9QZT7WPL",
    "camel": "HTTPServerError getUserById snake_case_identifier_name",
    "symbols": "!!??..,,;;::--__==++**&&||^^%%$$##@@~~``''\"\"<<>>//\\\\",
    "whitespace": "a  b   c\n\n\td    e\t\t\tf",
    "longword": "Pneumonoultramicroscopicsilicovolcanoconiosis antidisestablishmentarianism supercalifragilisticexpialidocious",
    "vowelly": "aeiou aeiouaeiou ooooo eeeee iaiaia",
}
# Rendered options as Laya builds them (render_options), measured as " " + text.
OPTIONS = {
    "digit_key": "10000000",
    "digit_key_10": "1234567890",
    "one_digit": "7",
    "level": "level 0: Routine",
    "noul_false": "false: no, the statement does not hold",
    "noul_true": "true: yes, the statement holds",
    "choice_desc": "billing: Payments and refunds",
    "bare_key": "bug",
    "cjk_key": "退款",
    "cjk_desc": "退款: 客户要求退款",
    "symbol_key": "<=5",
    "money": "$100.00",
    "percent": "50%",
    "hash": "#1",
    "emoji_key": "😀",
    "dash": "-",
    "json_desc": "a: {\"min\": 1, \"max\": 5}",
    "date": "2026-10-20",
    "cyrillic_key": "Возврат",
    "arabic_key": "استرداد",
    "upper_key": "P1",
    "snake_key": "needs_human_review",
}

def rows(mode):
    if mode == "head":
        return [(n, "noul question: " + t) for n, t in SAMPLES.items()]
    return [(n, " " + t) for n, t in {**OPTIONS, **SAMPLES}.items()]

def table():
    under = 0
    for mode in ("head", "option"):
        print(f"\n[{mode}] {'sample':14} {'chars':>5} {'est':>5} | {'english':>7} {'multi':>5} | margin")
        for name, text in rows(mode):
            e, r = text_tokens(text), real(text)
            m = min(e - v for v in r.values())
            under += m < 0
            print(f"  {name:20} {len(text):5} {e:5} | {r['english']:7} {r['multilingual']:5} | {m:+d}{'  UNDER' if m < 0 else ''}")
    print("\nunder-counts:", under)
    return under

def stress(seed):
    rng = random.Random(seed); n = u = 0; worst = 0
    for a in (string.ascii_lowercase, string.ascii_letters, string.ascii_lowercase + "    ", string.printable[:95]):
        for _ in range(2500):
            t = " " + "".join(rng.choice(a) for _ in range(rng.randint(1, 60)))
            e = text_tokens(t)
            for v in real(t).values():
                n += 1; u += e < v; worst = min(worst, e - v)
    for _ in range(500):
        ws = " ".join("".join(rng.choice(string.ascii_lowercase) for _ in range(rng.randint(2, 9))) for _ in range(rng.randint(5, 60)))
        t = "noul question: " + ws; e = text_tokens(t)
        for v in real(t).values():
            n += 1; u += e < v; worst = min(worst, e - v)
    print(f"seed {seed}: comparisons {n}, under {u}, worst {worst:+d}")

if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "table"
    if cmd == "table":
        sys.exit(1 if table() else 0)
    elif cmd == "heads":
        defaults = ["false: no, the statement does not hold", "true: yes, the statement holds"]
        for name in ("japanese", "json_compact", "ins288", "ins548", "ins808"):
            head = "noul question: " + SAMPLES[name]
            est = text_tokens(head) + sum(1 + min(48, text_tokens(" " + o)) for o in defaults)
            r = {k: v + sum(1 + min(48, real(" " + o)[k]) for o in defaults) for k, v in real(head).items()}
            print(f"{name:13} estimate {est:4}  real english {r['english']:4}  multilingual {r['multilingual']:4}")
    elif cmd == "stress":
        stress(int(sys.argv[2]) if len(sys.argv) > 2 else 99)
    elif cmd == "rust":
        mode = sys.argv[2] if len(sys.argv) > 2 else "head"
        for name, text in rows(mode):
            if name.startswith("ins"):
                continue
            r = real(text)
            # Option rows print the rendered option; its real count is of " " + it.
            shown = text[1:] if mode == "option" else text
            print(f"            ({json.dumps(shown, ensure_ascii=False)}, {r['english']}, {r['multilingual']}), // {name}")
    else:
        sys.exit(__doc__)
