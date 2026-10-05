PRIORITY_VALUES = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
CLASSIFICATION_VALUES = ["UNCLASSIFIED", "RESTRICTED", "SECRET", "TOP_SECRET"]
KEY_LIST = [
    "alert_id",
    "source",
    "title",
    "content",
    "priority",
    "classification",
    "lat",
    "lon",
    "timestamp",
    "status"
]

MIN_LAT = -90
MAX_LAT = 90

MIN_LON = -180
MAX_LON = 180

STATUS = "WAITING"
FIELDS_LENGTH = 10

AMAN_TITLE = [
    "זוהה שיגור רקטות",
    "זוהה שיגור טיל בליסטי",
    "זוהו הכנות לשיגור",
    "זוהה כלי טיס בלתי מאויש עוין",
    "תנועת כוחות חריגה סמוך לגבול",
    "שיבושי ניווט באזור"
]

MOSSAD_TITLE = [
    "התרעה על כוונה לפגוע ביעד ישראלי בחוץ לארץ",
    "זוהה נתיב הברחת אמצעי לחימה",
    "פעילות חריגה באתר אסטרטגי",
    "ניסיון כניסה של פעיל עוין לישראל",
    "העברת כספים לארגון טרור",
    "תנועת פעיל עוין בין מדינות"
]

PIKUD_HAOREF_TITLE = [
    "ירי רקטות וטילים",
    "חדירת כלי טיס עוין",
    "חדירת מחבלים",
    "התרעה מקדימה",
    "רעידת אדמה",
    "האירוע הסתיים"
]

SHABAK_TITLE = [
    "התרעה חמה לפיגוע",
    "תנועת מחבל מבוקש",
    "חשד לחדירה ליישוב",
    "רכב חשוד",
    "חשד לפעילות ריגול עבור גורם עוין",
    "גניבת אמצעי לחימה"
]

def validate_fields(msg:dict):
    if len(msg) != FIELDS_LENGTH:
        return False
    if list(msg.keys()) != KEY_LIST:
        return False
    if type(msg["lat"]) != float or type(msg["lon"]) != float:
        return False
    if msg["priority"] not in PRIORITY_VALUES:
        return False
    if msg["classification"] not in CLASSIFICATION_VALUES:
        return False
    if msg["lat"] < MIN_LAT or msg["lat"] > MAX_LAT:
        return False
    if msg["lon"] < MIN_LON or msg["lon"] > MAX_LON:
        return False
    if msg["status"] != STATUS:
        return False
    
    return True

def validate_title(msg:dict):
    match msg["source"]:
        case "aman":
            if msg["title"] not in AMAN_TITLE:
                return False
        case "mossad":
            if msg["title"] not in MOSSAD_TITLE:
                return False
        case "pikud-haoref":
            if msg["title"] not in PIKUD_HAOREF_TITLE:
                return False
        case "shabak":
            if msg["title"] not in SHABAK_TITLE:
                return False
        case _:
            return False
    return True

def validator(msg:dict):
    if not validate_fields(msg):
        return False
    if not validate_title(msg):
        return False
    return True
