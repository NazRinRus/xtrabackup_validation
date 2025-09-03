file_path = 'grastate.dat'
content = f"# GALERA saved state\nversion: 2.1\nuuid: 1\nseqno: -1\nsafe_to_bootstrap: 1"
with open(file_path, 'w') as grastate:
    grastate.write(content)
