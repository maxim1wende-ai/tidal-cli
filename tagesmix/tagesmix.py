# Tagesmix-Lernlogik v2 (4 Mixe pro Tag) – python3 tagesmix.py offen|plan|eintrag ...
import json, math, os, re, glob, sys, argparse, datetime as dt
ZYKLUS=["Sport","Soul & Funk","Indie & Alternative","Essen mit Gästen","Hard Rock & Metal","Pop seit 2000","Party","Jazz & Swing","Singer-Songwriter & Folk","Lernen & Fokus","80er & 90er Pop","Latin & Weltmusik","Autofahrt","Hip-Hop & R&B","Chanson","Morgens & Frühstück","Dance & Electronic","Kabarett & Lieder","Classic Rock & Blues","Italo-Pop","Disco & Boogie","Progressive Rock","Deutschrap","Reggae & Dancehall","Pop-Klassiker","Crooner","Klassik & Geistliches","Oldies","Schlager & Deutschpop","Soundtracks & Musicals"]
NAME={"standard":"Standard","kern":"Kern deines Geschmacks","bekannt":"Bekanntere Songs","geheimtipp":"Geheimtipps","treffer":"Mehr wie deine Treffer"}
WECHSEL=["treffer","kern","bekannt","geheimtipp","standard"]; PROBE=["treffer","kern","geheimtipp","bekannt","standard"]
def cats_on(d):
    n=(d-dt.date(2026,11,22)).days
    return [(("Party · Oldies" if (k//30)%2==0 else "Party · Modern") if ZYKLUS[k%30]=="Party" else ZYKLUS[k%30]) for k in range(4*n,4*n+4)]
def slug(s):
    s=s.lower()
    for a,b in (("ä","ae"),("ö","oe"),("ü","ue"),("ß","ss"),("&"," und ")): s=s.replace(a,b)
    return re.sub(r"[^a-z0-9]+","-",s).strip("-")
def docid(datum,kat): return f"{99999999-int(datum.replace('-',''))}-{slug(kat)}"
def prior(n): return max(0.5,min(3.0,(n or 60)/60))
def schwach(k,E): return sum(math.exp(-E)*E**i/math.factorial(i) for i in range(k+1))<0.25
def load(d,coll):
    out={}
    for f in glob.glob(os.path.join(d,"data","tagesmix",coll,"*.json")):
        try: out[os.path.basename(f)[:-5]]=json.load(open(f,encoding="utf-8"))
        except Exception as e: print("WARNUNG: nicht lesbar",f,e,file=sys.stderr)
    return out
def naechster(kat,nach):
    for i in range(1,41):
        d=nach+dt.timedelta(days=i)
        if kat in cats_on(d): return d.isoformat()
    return ""
def neu_state(): return {"S":0.0,"k":0,"stats":{s:[0,0] for s in NAME},"cur":None,"win":[],"hits":[],"last":None,"lastE":0,"size":None}
def E_of(st): return (2*prior(st["size"])+st["S"])/(2+st["k"])
def decide(st):
    cur=st["cur"] or "standard"; E=E_of(st)
    avail=[s for s in NAME if s!="treffer" or len(st["hits"])>=3 or s==cur]
    rate=lambda s:(st["stats"][s][1]+E)/(st["stats"][s][0]+20)
    trail=st["win"][-3:]; nf=sum(trail); new=cur; status=grund=None
    if nf>=2:
        new=max([s for s in avail if s!=cur],key=lambda s:(round(rate(s),9),-WECHSEL.index(s)))
        status="Kurswechsel"; grund=(f"Die letzten {nf}" if nf==len(trail) else f"{nf} der letzten {len(trail)}")+f" gehörten Mixe lagen deutlich unter dem, was in dieser Kategorie üblich ist – deshalb jetzt „{NAME[new]}“ statt „{NAME[cur]}“."
    else:
        better=[s for s in avail if s!=cur and st["stats"][s][0]>=10 and st["stats"][s][1]>=2 and rate(s)>=1.5*rate(cur)]
        if better:
            new=max(better,key=rate); n,h=st["stats"][new]; status="Kurswechsel"
            grund=f"Die Abwechslung „{NAME[new]}“ kam deutlich besser an ({h} Treffer aus {n} Songs) – sie wird zum Stamm."
    if status is None:
        if st["k"]==0: status,grund="noch offen","Noch kein gehörter Mix in dieser Kategorie – Standard-Ansatz."
        elif nf==1: status,grund="beobachten",f"Einer der letzten gehörten Mixe lag deutlich unter dem Üblichen – noch kein Grund zu wechseln, „{NAME[cur]}“ bleibt."
        elif st["last"] is not None and st["last"]>=max(2,st["lastE"]): status,grund="läuft gut",f"Zuletzt {st['last']} Treffer – „{NAME[cur]}“ bleibt."
        else: status,grund="normal",f"Im üblichen Rahmen – „{NAME[cur]}“ bleibt."
    abw=min([s for s in avail if s!=new],key=lambda s:(st["stats"][s][0],PROBE.index(s)))
    seeds=list(dict.fromkeys(reversed(st["hits"])))[:15] if "treffer" in (new,abw) else []
    return {"ansatz":new,"ansatzName":NAME[new],"abwechslung":abw,"abwechslungName":NAME[abw],"status":status,"grund":grund,"erwartung":round(E,1),"trefferSeeds":seeds}
def replay(mixe,ausw,heute,neu):
    ev={i:dict(a) for i,a in ausw.items()}; frisch=[]
    for i,hits in neu.items():
        if i in mixe and i not in ev:
            m=mixe[i]; ids=[str(h[0]) for h in hits]; st=set(map(str,m.get("stammIds") or [])); ab=set(map(str,m.get("abwechslungIds") or []))
            ev[i]={"datum":m["datum"],"kategorie":m["kategorie"],"slug":m.get("slug") or slug(m["kategorie"]),"treffer":len(ids),
                   "trefferStamm":len(set(ids)&st) if (st or ab) else len(ids),"trefferAbwechslung":len(set(ids)&ab),
                   "trefferIds":ids,"trefferTitel":" · ".join(str(h[1]) for h in hits if len(h)>1),"ausgewertetAm":heute}
            frisch.append(i)
    tag={}
    for e in ev.values(): tag[e["datum"]]=tag.get(e["datum"],0)+int(e.get("treffer") or 0)
    for i in frisch: ev[i]["gehoert"]=tag[ev[i]["datum"]]>=1
    state={}
    for i in sorted(mixe,key=lambda i:(mixe[i]["datum"],i)):
        m=mixe[i]; st=state.setdefault(m["kategorie"],neu_state()); st["size"]=m.get("playlistGroesse") or st["size"]
        a=m.get("ansatz") or "standard"; E=E_of(st)
        if a!=st["cur"]: st["cur"]=a; st["win"]=[]
        e=ev.get(i)
        if not e: continue
        st["hits"]+= [str(x) for x in e.get("trefferIds") or []]
        if e.get("gehoert"):
            t=int(e.get("treffer") or 0); f=schwach(t,E); e["fehlgriff"]=f
            st["win"].append(f); st["S"]+=t; st["k"]+=1; st["last"]=t; st["lastE"]=E
            st["stats"][a][0]+=int(m.get("songsStamm") or 20); st["stats"][a][1]+=int(e.get("trefferStamm") or 0)
            b=m.get("abwechslung")
            if b in NAME: st["stats"][b][0]+=int(m.get("songsAbwechslung") or 0); st["stats"][b][1]+=int(e.get("trefferAbwechslung") or 0)
        else: e["fehlgriff"]=False
        if i in frisch:
            d=decide(st)
            e.update({"erwartungDanach":d["erwartung"],"statusDanach":d["status"],"grundDanach":d["grund"],"naechsterAnsatz":d["ansatz"],"naechsterAnsatzName":d["ansatzName"],
                      "naechsteAbwechslung":d["abwechslung"],"naechsteAbwechslungName":d["abwechslungName"],"naechsterMix":naechster(m["kategorie"],dt.date.fromisoformat(heute))})
    return ev,frisch,state
def main():
    ap=argparse.ArgumentParser(); ap.add_argument("cmd"); ap.add_argument("--export"); ap.add_argument("--heute"); ap.add_argument("--treffer")
    ap.add_argument("--kat",action="append",default=[]); ap.add_argument("--out",default="tm_out"); ap.add_argument("--plan"); ap.add_argument("--playlist",default="")
    ap.add_argument("--stamm",default=""); ap.add_argument("--abw",default=""); a=ap.parse_args(); os.makedirs(a.out,exist_ok=True)
    if a.cmd=="eintrag":
        p=json.load(open(a.plan,encoding="utf-8")); k=a.kat[0].split("=")[0]; e=[x for x in p if x["kategorie"]==k][0]
        st=[x.strip() for x in a.stamm.split(",") if x.strip()]; ab=[x.strip() for x in a.abw.split(",") if x.strip()]
        doc={"datum":e["datum"],"kategorie":k,"slug":slug(k),"ansatz":e["ansatz"],"ansatzName":e["ansatzName"],"abwechslung":e["abwechslung"] if ab else "",
             "abwechslungName":e["abwechslungName"] if ab else "","songsStamm":len(st),"songsAbwechslung":len(ab),"stammIds":st,"abwechslungIds":ab,
             "playlistId":a.playlist,"playlistGroesse":e["playlistGroesse"],"erwartung":e["erwartung"],"status":e["status"],"grund":e["grund"],"hinweis":""}
        f=os.path.abspath(os.path.join(a.out,e["docId"]+".json")); json.dump(doc,open(f,"w",encoding="utf-8"),ensure_ascii=False)
        print(json.dumps({"op":"set","collection":"data/tagesmix/mixe","doc_id":e["docId"],"file_path":f},ensure_ascii=False)); return
    mixe=load(a.export,"mixe"); ausw=load(a.export,"auswertungen"); heute=a.heute
    if a.cmd=="offen":
        print(json.dumps([{"id":i,"kategorie":m["kategorie"],"datum":m["datum"],"trackIds":",".join(map(str,(m.get("stammIds") or [])+(m.get("abwechslungIds") or [])))}
                          for i,m in sorted(mixe.items()) if m["datum"]<heute and i not in ausw and (m.get("stammIds") or m.get("abwechslungIds"))],ensure_ascii=False,indent=1)); return
    neu=json.load(open(a.treffer,encoding="utf-8")) if a.treffer else {}
    ev,frisch,state=replay(mixe,ausw,heute,neu); writes=[]
    for i in frisch:
        f=os.path.abspath(os.path.join(a.out,"ausw-"+i+".json")); json.dump(ev[i],open(f,"w",encoding="utf-8"),ensure_ascii=False)
        writes.append({"op":"set","collection":"data/tagesmix/auswertungen","doc_id":i,"file_path":f})
        e=ev[i]; print(f"AUSWERTUNG {e['datum']} {e['kategorie']}: {e['treffer']} Treffer (Stamm {e['trefferStamm']}, Abwechslung {e['trefferAbwechslung']}), gehört={e['gehoert']}, Fehlgriff={e['fehlgriff']} → nächstes Mal: {e['naechsterAnsatzName']} + {e['naechsteAbwechslungName']} ({e['statusDanach']})")
    json.dump(writes,open(os.path.join(a.out,"writes_auswertungen.json"),"w",encoding="utf-8"),ensure_ascii=False)
    plan=[]
    for kv in a.kat:
        k,_,n=kv.rpartition("="); k=k or n; size=int(n) if n.isdigit() else None
        i=docid(heute,k)
        if i in mixe: print(f"HINWEIS {k}: für heute gibt es schon einen Protokoll-Eintrag – nicht neu schreiben."); continue
        st=state.get(k) or neu_state(); st["size"]=size or st["size"]; d=decide(st)
        d.update({"kategorie":k,"datum":heute,"docId":i,"playlistGroesse":size or st["size"]}); plan.append(d)
        print(f"PLAN {k}: Stamm = {d['ansatzName']} ({d['ansatz']}), Abwechslung = {d['abwechslungName']} ({d['abwechslung']}), Status {d['status']}, Erwartung {d['erwartung']}"+(f", trefferSeeds {','.join(d['trefferSeeds'])}" if d['trefferSeeds'] else "")+f"\n  Grund: {d['grund']}")
    json.dump(plan,open(os.path.join(a.out,"plan.json"),"w",encoding="utf-8"),ensure_ascii=False,indent=1)
    print("SCHREIBEN (ArtifactData batch, writes):",json.dumps(writes,ensure_ascii=False))
if __name__=="__main__": main()
