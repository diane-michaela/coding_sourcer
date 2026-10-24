"""Regenere le bookmarklet (.bookmarklet.txt, une ligne URL-encodee) et install.html
a partir du .js lisible. A relancer apres chaque modification du .js :

    python3 li-git-bookmarklets/build.py

puis ouvrir install.html et refaire le glisser-deposer du bouton.

Si li-git-bookmarklets/.env contient GITHUB_TOKEN=ghp_..., une version avec le token integre
est aussi generee (install_local.html + *_local.bookmarklet.txt, ignores par git) :
LinkedIn efface le localStorage qu'il ne reconnait pas, le token doit donc etre dans le favori.
"""
import html
import urllib.parse
from pathlib import Path

HERE = Path(__file__).resolve().parent
BOOKMARKLETS = [("li → github", "linkedin_to_github"), ("github → li", "github_to_linkedin")]
PLACEHOLDER = "__GH_TOKEN__"


SAFE = "".join(chr(c) for c in range(32, 127) if chr(c) not in "%#")


def one_line(src):
    # JS lisible : seuls %, # et les caracteres non ASCII sont encodes (sinon Chrome coupe l'URL)
    return "javascript:" + urllib.parse.quote(" ".join(l.strip() for l in src.splitlines() if l.strip()), safe=SAFE)


def read_token():
    env = HERE / ".env"
    if not env.exists():
        return ""
    for line in env.read_text(encoding="utf-8").splitlines():
        key, _, value = line.partition("=")
        if key.strip() == "GITHUB_TOKEN":
            return value.strip().strip("'\"")
    return ""


def write_install(buttons, filename, token_note):
    page = f"""<!doctype html><html lang="fr"><head><meta charset="utf-8"><title>Installer le bookmarklet</title>
<style>body{{font-family:-apple-system,sans-serif;max-width:640px;margin:40px auto;padding:0 16px;line-height:1.5}}
a.bm{{display:inline-block;padding:10px 18px;margin:8px 8px 8px 0;background:#2563eb;color:#fff;border-radius:8px;text-decoration:none;font-weight:600}}
ol li{{margin:6px 0}}</style></head><body>
<h1>Installer le bookmarklet</h1>
<ol><li>Affiche la barre de favoris : <b>Cmd+Shift+B</b>.</li>
<li><b>Fais glisser</b> les boutons bleus dans la barre de favoris (ne clique pas dessus ici).</li></ol>
<p>{' '.join(buttons)}</p>
<p>Utilisation : sur un profil LinkedIn (classique ou Recruiter Lite), clique sur le favori. Un panneau s'ouvre :
liens GitHub deja presents sur la page, bouton « Classer les comptes GitHub (API) », puis recherches GitHub et Google pretes a cliquer.
Nom, lieu et entreprise sont lus sur le profil et restent modifiables. Re-cliquer le favori ferme le panneau.</p>
<p><b>github → li</b> : sur un profil GitHub (github.com/pseudo), clique sur le favori. Le bouton « Trouver le vrai nom » lit le nom
et l'email signés dans ses commits publics (comme le .patch d'un commit), puis propose les recherches LinkedIn et Google.</p>
<p>{token_note}</p>
</body></html>"""
    (HERE / filename).write_text(page, encoding="utf-8")


def main():
    token = read_token()
    buttons, local_buttons = [], []
    for label, name in BOOKMARKLETS:
        src = (HERE / f"{name}.js").read_text(encoding="utf-8")
        code = one_line(src)
        (HERE / f"{name}.bookmarklet.txt").write_text(code + "\n", encoding="utf-8")
        buttons.append(f'<a class="bm" href="{html.escape(code, quote=True)}">{label}</a>')
        if token:
            local = one_line(src.replace(PLACEHOLDER, token))
            (HERE / f"{name}_local.bookmarklet.txt").write_text(local + "\n", encoding="utf-8")
            local_buttons.append(f'<a class="bm" href="{html.escape(local, quote=True)}">{label}</a>')
    write_install(buttons, "install.html",
                  "Token GitHub (demande au clic sur le bouton API) : github.com/settings/tokens &gt; Generate new token (classic), "
                  "<b>aucune case cochee</b>. Pour ne plus le saisir, mets-le dans li-git-bookmarklets/.env (GITHUB_TOKEN=...) "
                  "et utilise install_local.html.")
    if token:
        write_install(local_buttons, "install_local.html",
                      "Cette version contient ton token GitHub : ne la partage pas (fichier ignore par git).")
    print("bookmarklet et install.html regeneres" + (" + version locale avec token" if token else " (pas de .env : pas de version avec token)"))


if __name__ == "__main__":
    main()
