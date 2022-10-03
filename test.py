for a in range(97, 123):
    location = f"S-{str(chr(a)).upper()}"
    for i in range(1, 5):
        for e in range(1, 14):
            for j in range(1, 5):
                print(f"{location}{i:02d}-{e:02d}-{j:02d}")

l = ["S-P58","S-P59","S-CK1","S-CK2","S-OVER-Y01","S-OVER-Y02","S-OVER-Y03","S-OVER-Y04","S-OVER-Y05","S-OVER-Y06","S-OVER1","S-OVER2","S-OVER3","S-HOLD","S-REPALLET","S-RECHECK","SNON","S-XXX","S-PLOUT","-"]
for i in l:
    print(i)