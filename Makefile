version:
	rustc --version
	wasm-pack --version

build:
	$(MAKE) version

	wasm-pack build --scope pixelcities --target web
	sed -i "s|fetch(input)|fetch(input, {integrity: \"sha384-$$(cat pkg/datafusion_wasm_bg.wasm | openssl dgst -sha384 -binary | openssl base64 -A)\"})|g" pkg/datafusion_wasm.js
