file_path="./app.yaml"

sed -i 's|service_user_credentials_file: /integration_tests/rag_evals/gdrive_indexer.json|service_user_credentials_file: ./gdrive_indexer.json|' "$file_path"

pytest test_eval.py
