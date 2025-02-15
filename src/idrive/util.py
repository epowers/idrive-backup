def lstrip1(string: str, char: str) -> str:
    return string[1:] if len(string) and string[0] == char else string

def rstrip1(string: str, char: str) -> str:
    return string[:-1] if len(string) and string[-1] == char else string

def strip1(string: str, char: str) -> str:
    return lstrip1(rstrip1(string, char), char)
