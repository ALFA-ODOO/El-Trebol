import sys
from clientes_sync import actualizar_clientes

def main():
    if len(sys.argv) > 1:
        codigos = sys.argv[1].split(",")
        codigos = [c.strip() for c in codigos if c.strip()]
        if codigos:
            actualizar_clientes(filtrar_codigos=codigos)
        else:
            print("⚠️ No se ingresaron códigos válidos.")
    else:
        print("Uso: actualizar_clientes_por_codigo.exe CODIGO1,CODIGO2,...")

if __name__ == "__main__":
    main()
