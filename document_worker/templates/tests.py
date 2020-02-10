

def not_empty(x):
    if hasattr(x, '__len__'):
        return len(x) > 0
    return x is not None


tests = {
    'not_empty': not_empty
}
