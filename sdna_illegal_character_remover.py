import os
import argparse
import re
from datetime import datetime


# root = "D:\\storageDNA\\mnt"
# files = ['     ms@@#$$^^&mB.mdb      ', '    ms $ mB.pbr   ', '    msm ## MMB.mdb    ', '    msmM@#$%  MOB.mdb    ']
# dirs = ['  134-Source  ','  AVID&*-EOD-01  ','  Avid^ MediaFiles  ','  MXF  ','  Media-$%01-OP-1  ']


def txt_file_to_regex_pattern(txt_file_path):
    with open(txt_file_path,'r') as txt:
        symobles =  ''.join(text.strip() for text in txt)
        # pattern = f"[{re.escape(symobles)}]"
        pattern = "[{}]".format(re.escape(symobles))
        return pattern

def open_csv_file(given_filename):
    file_given = open(given_filename,"w")
    file_given.write("illegal_Name,legal_Name"+ "\n")
    return file_given

def close_csv_file(file_given):
    file_given.close()

def append_to_csv_file(file_given, data):
    file_given.write(data + '\n')
    
def illegal_char(target_path,dry_run,csv_file_path,pattern):
    
    os.makedirs(csv_file_path, exist_ok=True)
    file_given = os.path.join(csv_file_path,f"illegal_char_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv")
    file_given = open_csv_file(file_given)
    try:
        for root, dirs, files in os.walk(target_path):

            for file_name in files:
                file_path_illegal = os.path.join(root,file_name)
                filename = file_name.strip().replace("  "," ")
                filename = re.sub(pattern,"",filename)
                file_path_legal = os.path.join(root,filename)
                if file_path_legal != file_path_illegal:
                    if not dry_run :
                        try:
                            os.rename(file_path_illegal, file_path_legal)
                            append_to_csv_file(file_given,f'{file_path_illegal},{file_path_legal}')
                        except Exception as e:
                            print(f"Faild to rename:{e}")
                    else:
                        append_to_csv_file(file_given,f'{file_name},{filename}')
                
            for i ,dir_name in enumerate(dirs):
                dir_path_illegal = os.path.join(root,dir_name)
                dirname = dir_name.strip().replace("  "," ")
                dirname = re.sub(pattern,"",dirname)
                dir_path_legal = os.path.join(root,dirname)
                if dir_path_legal != dir_path_illegal:
                    data = f'{dir_path_illegal},{dir_path_legal}'
                    if not dry_run :
                        try:
                            os.rename(dir_path_illegal, dir_path_legal)
                            append_to_csv_file(file_given,f'{dir_path_illegal},{dir_path_legal}') 
                            dirs[i] = dirname
                        except Exception as e:
                            print(f"Faild to rename:{e}")
                    else:
                        append_to_csv_file(file_given,f'{dir_name},{dirname}')
                
        close_csv_file(file_given)
        return True
    
    except Exception as e:
        print(f"An Error occurd: {e}")
        return False
    
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-t','--target',help='targetpath')
    parser.add_argument('-c','--csv_file',help='csvfilepath There generated csv file is stored.')
    parser.add_argument('-f','--txt_file',help='txtfilepath with symbols that you want to remove')
    parser.add_argument('--dry_run',action='store_true',help='dry_run')
   

    args = parser.parse_args()
    target_path = args.target
    csv_file_path = args.csv_file
    txt_file_path = args.txt_file
    dry_run = args.dry_run

    if target_path is None or txt_file_path is None or csv_file_path is None:
        print('Target path (-t <targetpath> ) , Csv file path (-c <csv_file>) and Txt file (-f <txt_file>) options are required.')
        exit(1)
            
    regex_patten = txt_file_to_regex_pattern(txt_file_path)
    csv_file = illegal_char(target_path,dry_run,csv_file_path,regex_patten)
    if csv_file:
        print(f'CSV file created')
        exit(0)
    else:
        print("Failed to create an CSV file.")
        exit(1)

