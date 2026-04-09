#!/bin/sh
# Inyecta Google Analytics en el index.html de Streamlit antes de arrancar.
# Al estar en el HTML base (no en un iframe), GA funciona en la página real.
#!/bin/sh
INDEX=/usr/local/lib/python3.12/site-packages/streamlit/static/index.html

if [ -n "$GA_MEASUREMENT_ID" ]; then
    if ! grep -q "googletagmanager" "$INDEX"; then
        python3 - <<EOF
index = "$INDEX"
ga_id = "$GA_MEASUREMENT_ID"
snippet = f"""<script async src="https://www.googletagmanager.com/gtag/js?id={ga_id}"></script>
<script>window.dataLayer=window.dataLayer||[];function gtag(){{dataLayer.push(arguments);}}gtag('js',new Date());gtag('config','{ga_id}');</script>"""

with open(index, "r") as f:
    html = f.read()

html = html.replace("</head>", snippet + "\n  </head>", 1)

with open(index, "w") as f:
    f.write(html)

print(f"GA inyectado correctamente (ID: {ga_id})")
EOF
    else
        echo "GA ya estaba presente"
    fi
fi

exec streamlit run app.py \
    --server.port=8501 \
    --server.address=0.0.0.0 \
    --server.headless=true

