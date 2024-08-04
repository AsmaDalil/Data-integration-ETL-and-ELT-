import subprocess

subprocess.run(['hadoop', 'fs', '-put', 'C:\Users\HP\OneDrive - Al Akhawayn University in Ifrane\Documents\Ma_DataEng\projectOnw\dataELT.csv', 'C:\Users\HP\Downloads\hadoop-3.3.6-src\bin\hadoop'], check=True)
