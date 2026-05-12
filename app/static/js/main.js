document.addEventListener("DOMContentLoaded", () => {
    // Mostra nome do arquivo selecionado nas dropzones
    function bindFilename(inputId, labelId) {
        const input = document.getElementById(inputId);
        const label = document.getElementById(labelId);
        if (!input || !label) return;
        input.addEventListener("change", () => {
            label.textContent = input.files[0] ? input.files[0].name : "";
        });
    }
    bindFilename("parametros",    "fn-parametros");
    bindFilename("dados",         "fn-dados");
    bindFilename("relatorio_pdf", "fn-relatorio_pdf");

    // Spinner ao submeter
    const form    = document.getElementById("form-upload");
    const spinner = document.getElementById("spinner");
    const btn     = document.getElementById("btn-analisar");

    if (form && spinner) {
        form.addEventListener("submit", () => {
            spinner.classList.add("active");
            if (btn) { btn.disabled = true; btn.textContent = "Processando…"; }
        });
    }
});
