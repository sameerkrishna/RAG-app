var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g = Object.create((typeof Iterator === "function" ? Iterator : Object).prototype);
    return g.next = verb(0), g["throw"] = verb(1), g["return"] = verb(2), typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (g && (g = 0, op[0] && (_ = 0)), _) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import path from 'path';
import { fileURLToPath } from 'url';
import fs from 'fs';
var __dirname = path.dirname(fileURLToPath(import.meta.url));
function expressPlugin() {
    var app;
    return {
        name: 'express-plugin',
        configureServer: function (server) {
            return __awaiter(this, void 0, void 0, function () {
                var dotenv, expressApp;
                return __generator(this, function (_a) {
                    switch (_a.label) {
                        case 0: return [4 /*yield*/, import('dotenv')];
                        case 1:
                            dotenv = _a.sent();
                            dotenv.config();
                            return [4 /*yield*/, import('./server/app.js')];
                        case 2:
                            expressApp = (_a.sent()).default;
                            app = expressApp;
                            server.middlewares.use('/api', function (req, res, next) {
                                var _a;
                                // ✅ Patch SSE routes to flush immediately — prevents Vite buffering tokens
                                if ((_a = req.url) === null || _a === void 0 ? void 0 : _a.startsWith('/chat')) {
                                    res.setHeader('X-Accel-Buffering', 'no');
                                    var originalWrite_1 = res.write.bind(res);
                                    res.write = function (chunk) {
                                        var result = originalWrite_1(chunk);
                                        if (typeof res.flush === 'function')
                                            res.flush();
                                        return result;
                                    };
                                }
                                app(req, res, next);
                            });
                            return [2 /*return*/];
                    }
                });
            });
        },
    };
}
function copyNetlifyFiles() {
    return {
        name: 'copy-netlify-files',
        closeBundle: function () {
            // Copy _redirects
            var redirectsSrc = path.resolve(__dirname, 'dist/_redirects');
            if (fs.existsSync(redirectsSrc)) {
                console.log('✅ _redirects exists in dist');
            }
            // Copy netlify.toml
            var netlifyToml = path.resolve(__dirname, 'netlify.toml');
            var netlifyTomlDest = path.resolve(__dirname, 'dist/netlify.toml');
            if (fs.existsSync(netlifyToml)) {
                fs.copyFileSync(netlifyToml, netlifyTomlDest);
                console.log('✅ netlify.toml copied to dist');
            }
            // Copy seed_documents folder to dist
            var seedSrc = path.resolve(__dirname, 'seed_documents');
            var seedDest = path.resolve(__dirname, 'dist/seed_documents');
            if (fs.existsSync(seedSrc)) {
                fs.mkdirSync(seedDest, { recursive: true });
                var files = fs.readdirSync(seedSrc);
                files.forEach(function (file) {
                    var srcFile = path.join(seedSrc, file);
                    var destFile = path.join(seedDest, file);
                    if (fs.statSync(srcFile).isFile()) {
                        fs.copyFileSync(srcFile, destFile);
                    }
                });
                console.log("\u2705 seed_documents copied to dist (".concat(files.length, " files)"));
            }
            // Copy google_credentials folder to dist
            var credsSrc = path.resolve(__dirname, 'google_credentials');
            var credsDest = path.resolve(__dirname, 'dist/google_credentials');
            if (fs.existsSync(credsSrc)) {
                fs.mkdirSync(credsDest, { recursive: true });
                var files = fs.readdirSync(credsSrc);
                files.forEach(function (file) {
                    var srcFile = path.join(credsSrc, file);
                    var destFile = path.join(credsDest, file);
                    if (fs.statSync(srcFile).isFile()) {
                        fs.copyFileSync(srcFile, destFile);
                    }
                });
                console.log("\u2705 google_credentials copied to dist (".concat(files.length, " files)"));
            }
        }
    };
}
export default defineConfig({
    plugins: [react(), expressPlugin(), copyNetlifyFiles()],
    resolve: {
        alias: {
            '@': path.resolve(__dirname, './src'),
        },
    },
    server: {
        port: 5173,
    },
});
