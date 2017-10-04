import os
import dropbox
import dropbox.oauth
import sys
import hashlib
import sqlite3
from dropbox_content_hasher import DropboxContentHasher

piece = 10 * 1024 * 1024
block = 4 * 1024 * 1024
database = []
dbIndex = 0
isAuth = False

def main():
    conn = create_connection('database.db')
    create_tables(conn)
    c = conn.cursor()
    c2 = conn.cursor()
    if not isAuth:
        dbx = Auth()
    cursor1 = c2.execute('SELECT max(ROWID) FROM results')
    max_id = cursor1.fetchone()[0]
    cursor2 = c.execute('SELECT * FROM results WHERE ROWID=?', (max_id,))
    result = cursor2.fetchone()[0]
    if(max_id > 0 and result == 'incomplete'):
        print("Detected unfished upload sequence.")
        choice = input('Enter 1 if you would like to resume, or hit enter to continue with a new session: ').strip()
        if(choice == '1'):
            c.execute('SELECT * FROM files')
            for row in c:
                result = upload(dbx, row[0])
                if result == 1:
                    #skip file
                    continue
                if result == 2:
                    #hash mismatch
                    upload(dbx, row)
                if result == 0:
                    #success
                    c2.execute("DELETE from files WHERE path=?", (row[0],))
                    conn.commit()
            temp = ('complete')
            c.execute('INSERT INTO results VALUES(?)', (temp,))
        else:
            c.execute("DELETE FROM files")
        conn.commit()



    while True:
        filepath = input('Enter path for folder: ')
        if not os.path.exists(filepath):
            print("The Directory doesn't exist")
        for root, dirs, files in os.walk(filepath, topdown=False):
            for name in files:
                path = os.path.join(root, name)
                path = path.replace("\\", "/")
                temp = (path, 'none')
                c.execute('INSERT INTO files VALUES(?, ?)', temp)
        temp = ('incomplete')
        c.execute('INSERT INTO results VALUES(?)', (temp,))
        conn.commit()

        c.execute('SELECT * FROM files')
        for row in c:
            try:
                result = upload(dbx, row[0])
                if result == 1:
                    #skip file
                    continue
                if result == 2:
                    #hash mismatch
                    upload(dbx, row)
                if result == 0:
                    #success
                    c2.execute("DELETE from files WHERE path=?", (row[0],))
                    conn.commit()
            except BaseException as err:
                temp = err, row[0]
                c2.execute('UPDATE results SET error = ? WHERE path = ?', temp)
        cursor = c2.execute('SELECT max(ROWID) FROM results')
        max_id = cursor.fetchone()[0]
        temp = ('complete', max_id)
        c.execute('UPDATE results SET result = ? WHERE ROWID = ?', temp)
        conn.commit()

        
def upload(dbx, row):
    ignored = "desktop.ini", "thumbs.db", ".ds_store", "icon\r", ".dropbox", ".dropbox.attr"
    meta = dropbox.files.FileMetadata()
    try:
        size = os.path.getsize(row)
    except BaseException as err:
        raise err
    path = row[2:]
    for ignore in ignored:
        if ignore in path:
            return 1
    try:
        with open(row, 'rb') as item:
            if size > piece:
                count = 0
                try:
                    print("Attempting to upload large file " + path)
                    upload_session = dbx.files_upload_session_start(item.read(piece))
                    print("Created Session")
                    curs = dropbox.files.UploadSessionCursor(
                        session_id=upload_session.session_id, offset=item.tell())
                    commit = dropbox.files.CommitInfo(path=path)
                    while item.tell() < size:
                        count += 1
                        if (size - item.tell()) <= piece:
                            print("finishing piece")
                            meta = dbx.files_upload_session_finish(item.read(piece), curs, commit)
                        else:
                            print("uploading piece: {0:.2f}%".format(((count*piece)/size)*100))
                            dbx.files_upload_session_append_v2(item.read(piece), curs)
                        curs.offset = item.tell()
                except BaseException as err:
                    raise err
            else:
                print("Attempting to upload " + path)
                meta = dbx.files_upload(item.read(), path)
            if(meta.content_hash == db_hash(row)):
                return 0
            else:
                return 2
    except BaseException as err:
        raise err

def db_hash(file_name):
    hasher = DropboxContentHasher()
    file = open(file_name, 'rb')
    while True:
        chunk = file.read(block)
        if len(chunk) == 0:
            break
        hasher.update(chunk)
    return hasher.hexdigest()
def Auth():

    APP_KEY = ""
    APP_SECRET = ""
    auth_flow = dropbox.oauth.DropboxOAuth2FlowNoRedirect(APP_KEY, APP_SECRET)

    authorize_url = auth_flow.start()
    print("1. Go to: " + authorize_url)
    print("2. Click \"Allow\" (you might have to log in first).")
    print("3. Copy the authorization code.")
    auth_code = input("Enter the authorization code here: ").strip()

    try:
        oauth_result = auth_flow.finish(auth_code)
    except Exception:
        print('Error: %s')
        return
    dbx = dropbox.Dropbox(oauth_result.access_token)
    isAuth = True
    return(dbx)

def create_connection(path):
    try:
        conn = sqlite3.connect(path)
        return conn
    except BaseException as err:
        print(err)
    return 

def create_tables(conn):
    if conn is not None:
        curs = conn.cursor()
        curs.execute('''CREATE TABLE IF NOT EXISTS files(path text, error text)''')
        curs.execute('''CREATE TABLE IF NOT EXISTS results(result text)''')
    else:
        print("error")

if __name__ == '__main__':
    main()