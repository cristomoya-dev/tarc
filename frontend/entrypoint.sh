#!/bin/sh
# Inyecta Google Analytics en el index.html de Streamlit antes de arrancar.
# Al estar en el HTML base (no en un iframe), GA funciona en la página real.
if [ -n "$GA_MEASUREMENT_ID" ]; then
    INDEX=$(python -c "import streamlit, os; print(os.path.join(os.path.dirname(streamlit.__file__), 'static', 'index.html'))")
    # Solo parchear si todavía no está inyectado
    if ! grep -q "googletagmanager" "$INDEX"; then
        GA_SNIPPET="<script async src=\"https://www.googletagmanager.com/gtag/js?id=${GA_MEASUREMENT_ID}\"></script><script>window.dataLayer=window.dataLayer||[];function gtag(){dataLayer.push(arguments);}gtag('js',new Date());gtag('config','${GA_MEASUREMENT_ID}');</script>"
        sed -i "s|</head>|${GA_SNIPPET}</head>|" "$INDEX"
        echo "GA inyectado en $INDEX (ID: $GA_MEASUREMENT_ID)"
    fi
fi

exec streamlit run app.py \
    --server.port=8501 \
    --server.address=0.0.0.0 \
    --server.headless=true
