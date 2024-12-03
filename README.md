# MoneyLineGANs

This project aims to develop and test a Generative Adversarial Network (GAN),
to generate synthetic moneyline betting data which accurately reflects the dynamics of the Massachusetts sports betting markets. By experimenting with
different model architectures, we hope to demonstrate that the generated synthetic data can mimic real-world patterns and provide insights into market
inefficiencies and arbitrage opportunities.

This repo contains all the code for scarping real-time moneyline data from The Odds API. The scripts preprocess the raw data and stores it to AWS S3 with aggregated visualizations. It also contains the implementation and testing of W-GANs with various architectures and model configurations.

## Model Architecture 

The GAN consists of two neural networks: a generator that produces synthetic betting odds and a discriminator that evaluates whether the data is real or generated. By iteratively improving both networks, the generator learns to produce increasingly realistic betting odds. We use the W-GAN architecture which is known to be easier to train than traditional GANs. In addition, the original W-GAN implementation enforces a Lipschitz constraint on the weights matrix via parameter clipping which is known to have poor performance. We compare this original implementation to another W-GAN which enforces Lipschitz via spectral normalization.

## Problem Formulation

Consider discrete measurements of moneyline data 

$$
\mathbf{x} = \begin{pmatrix}\text{+ moneyline}\\
\text{- moneyline}\end{pmatrix}
$$

at the start of a game $S_0$. We have our dataset $D = \left\\{\mathbf{x}_i\right\\}\_{i=1}^N$ representing moneyline observations at $S_0$ for different NBA games. Let $\mathbf{X}$ be the space of all moneylines $\mathbf{x}$ at $S_0$. We want to estimate a probability distribution $\mathbb{P}\_\theta(\mathbf{X})$ such that

$$
\mathbb{P}\_r(\mathbf{X}) \approx \mathbb{P}_\theta(\mathbf{X})
$$

where $\mathbb{P}\_r$ is the real data distribution. Essentially, we want to find parameters $\theta$ that minimize the distance between our constructed distribution $\mathbb{P}\_\theta$ and our observed data distribution $\mathbb{P}\_r$.

$$
\min_{\theta} D \left(\mathbb{P}\_r|| \mathbb{P}_\theta\right)
$$

We can construct $\mathbb{P}_\theta$ using a Wasserstein GAN. This architecture minimizes the Wasserstein-1 (or EM) distance metric 

$$
W(\mathbb{P}\_r, \mathbb{P}\_\theta)  = \inf_{\gamma \in \Pi(\mathbb{P}\_r, \mathbb{P}\_\theta)} \mathbb{E}_{(x,y) \sim \gamma}[\|\|x - y\|\|]
$$

where $\Pi(\mathbb{P}\_r\mathbb{P}_\theta)$ denotes the set of all joint distributions $\gamma(x,y)$ whose marginals are respectively $\mathbb{P}\_r$ and $\mathbb{P}\_\theta$. Intuitively, $\gamma(x,y)$ indicates how much "mass" must be transported from $x$ to $y$ to transform the distributions $\mathbb{P}\_r$ into the distribution $\mathbb{P}\_\theta$. The EM distance is essentially the "cost" of the optimal transport plan (Arjovsky et. al 2017). By the Kantorovich-Rubinstein duality (Villani et. al 2008), we can calculate our Wasserstein-1 distance metric using 

$$
W(\mathbb{P}\_r, \mathbb{P}\_\theta) = \sup_{\|\|f\|\|\_L \le 1} \mathbb{E}_{x \sim \mathbb{P}\_r}[f(x)] - \mathbb{E}\_{x\sim \mathbb{P}\_\theta}[f(x)]
$$

where $\|\|f\|\|_L \le1$ means that $f(x)$ must be 1-Lipschitz. Essentially

$$\forall x,y: |f(x) - f(y)| \le |x-y|$$

which can be interpreted as the function $f$ must bounded by a linear function with a slope of $1$. There are several ways to enforce Lipschitz in the context of training GANs. In this paper, we compare using weight clipping (Arjovsky et. al 2017) to using spectral normalization (Miyato et. al 2018) to enforce this constraint. 

## W-GAN Training

To train the W-GAN, consider two networks $C_\phi(x)$ and $G_\theta(z)$. First, we sample random vectors $z \sim N(0, I)$ and pass them through the generator network $G$. We then pass these the outputs $G(z)$ and real observations from our data set to the critic network $C$ which attempts to classify samples as real or generated. Optimizing both networks

$$
\min_{\theta}\max\_{\phi}\ \mathbb{E}\_{x\sim p\_{data}}[C_\phi(x)] - \mathbb{E}\_{z\sim N(0, I)}[C_\phi (G_\theta(z))]
$$

results in a generator network that outputs synthetic data which mimics the real data distribution. We only enforce Lipschitz on $C_\phi(x)$ via weight clipping or spectral normalization.

## Wasserstein GAN Training Algorithm (with weight clipping)

**Inputs:**  
- Learning rate `α`  
- Clipping parameter `c`  
- Batch size `m`  
- Number of critic iterations `n_critic`  
- Initial critic parameters `w_0`  
- Initial generator parameters `θ_0`  

**Algorithm:**

1. **While** generator parameters `θ` have not converged:
    1. **For** `t = 1` to `n_critic`:
        1. Sample `{x^(i)}` from the real data distribution `P_r`, with batch size `m`.
        2. Sample `{z^(i)}` from the prior distribution `p(z)`, with batch size `m`.
        3. Compute the gradient for the critic: $g_w = \nabla_w [\frac{1}{m} \sum_{i=1}^m f_w(x^{(i)}) - \frac{1}{m} \sum_{i=1}^m f_w(g_θ(z^{(i)}))]$
        4. Update critic parameters using RMSProp: $w \leftarrow w + \alpha \cdot \text{RMSProp}(w, g_w)$
        5. Clip critic parameters: $w \leftarrow \text{clip}(w, -c, c)$
    2. Sample `{z^(i)}` from the prior distribution `p(z)`, with batch size `m`.
    3. Compute the gradient for the generator: $g_θ = -\nabla_θ \frac{1}{m} \sum_{i=1}^m f_w(g_θ(z^{(i)}))$
    4. Update generator parameters using RMSProp: $θ \leftarrow θ - α \cdot \text{RMSProp}(θ, g_θ)$

**End While**
