# Tagesmix

Lernlogik für den täglichen Tidal-Tagesmix: jeden Morgen vier Mixe mit je 20 Empfehlungen, passend zu einer eigenen Kategorie-Playlist. Das Skript wertet aus, welche Songs aus den Mixen vom Vortag in die Favoriten oder Playlists übernommen wurden, und entscheidet daraus je Kategorie den Ansatz für den nächsten Mix (15 Songs Stamm, 5 Songs Abwechslung).

Die geplante Aufgabe „Tidal-Tagesmix“ in Claude holt sich das Skript bei jedem Lauf frisch aus diesem Repo. Eine Änderung hier wirkt also ab dem nächsten Morgen.

## Befehle

```bash
# Noch nicht ausgewertete Mixe aus dem Protokoll-Export (JSON-Liste mit id und trackIds)
python3 tagesmix.py offen --export <export-ordner> --heute JJJJ-MM-TT

# Auswertung von gestern + Plan für heute (je Kategorie ein --kat, optional mit Playlist-Größe)
python3 tagesmix.py plan --export <export-ordner> --heute JJJJ-MM-TT --treffer treffer.json \
  --kat "Sport=58" --kat "Soul & Funk=41" --kat "Indie & Alternative" --kat "Essen mit Gästen=33" --out out

# Protokoll-Zeile für einen fertig gebauten Mix
python3 tagesmix.py eintrag --plan out/plan.json --kat "Sport" --playlist <playlistId> \
  --stamm <ids,kommagetrennt> --abw <ids,kommagetrennt> --out out
```

- `--export` ist der Ordner, in den die Aufgabe die Sammlungen `data/tagesmix/mixe` und `data/tagesmix/auswertungen` aus dem Dashboard „Tagesmix-Auswertung“ exportiert.
- `treffer.json` hat das Format `{"<id des Mixes>": [["<trackId>", "<Titel> – <Interpreten>"], …]}`. Ein geprüfter Mix ohne Treffer bekommt `[]`.
- `plan` schreibt `out/plan.json` und `out/writes_auswertungen.json` und gibt die Zeilen AUSWERTUNG, PLAN und SCHREIBEN aus.

Playlist-IDs, Fallback-Seeds und die Sperrliste stehen nicht hier, sondern in der Beschreibung der geplanten Aufgabe.

## Test

```bash
python3 tagesmix/test_tagesmix.py
```
