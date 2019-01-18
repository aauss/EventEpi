

def _remove_guillemets(string):
    # Remove the first and last guillemets. Found in URLs of edb
    try:
        string = re.sub(r'<', '', string, 1)
        string = re.sub(r'>', '', string[::-1], 1)
        string = string[::-1]
    except TypeError:
        string = np.nan
return string