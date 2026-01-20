import torch 
from torchmetrics.functional import structural_similarity_index_measure as ssim


def evaluate_epoch(model, loader, loss_fn, device):
    model.eval()
    loss_vals, mse_vals, mae_vals, ssim_vals = [], [], [], []

    with torch.no_grad():
        for imgs in loader:
            imgs = imgs.to(device)
            recon = model(imgs)

            loss = loss_fn(recon, imgs)

            mse = ((imgs - recon) ** 2).mean(dim=(1, 2, 3))
            mae = (imgs - recon).abs().mean(dim=(1, 2, 3))
            s = ssim(recon, imgs, data_range=1.0, reduction="none")

            loss_vals.append(loss.detach().cpu())
            mse_vals.append(mse.detach().cpu())
            mae_vals.append(mae.detach().cpu())
            ssim_vals.append(s.detach().cpu())

    mse_all = torch.cat(mse_vals)
    mae_all = torch.cat(mae_vals)
    ssim_all = torch.cat(ssim_vals)

    loss_mean = torch.stack(loss_vals).mean().item()

    return {
        "loss": loss_mean,
        "mse": mse_all.mean().item(),
        "mae": mae_all.mean().item(),
        "ssim": ssim_all.mean().item()
    }
